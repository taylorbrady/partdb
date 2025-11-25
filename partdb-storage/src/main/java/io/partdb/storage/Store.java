package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.KVStorage;
import io.partdb.common.LeaseProvider;
import io.partdb.storage.compaction.CompactionExecutor;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.compaction.LeveledCompactionStrategy;
import io.partdb.storage.compaction.Manifest;
import io.partdb.storage.compaction.ManifestData;
import io.partdb.storage.compaction.SSTableMetadata;
import io.partdb.storage.memtable.Memtable;
import io.partdb.storage.memtable.SkipListMemtable;
import io.partdb.storage.sstable.SSTableConfig;
import io.partdb.storage.sstable.SSTableReader;
import io.partdb.storage.sstable.SSTableWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class Store implements KVStorage {

    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final int MAX_IMMUTABLE_MEMTABLES = 4;

    private final Path dataDirectory;
    private final StoreConfig config;
    private final LeaseProvider leaseProvider;
    private final Object memtableLock = new Object();
    private final Object manifestLock = new Object();
    private final Semaphore flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
    private final AtomicLong sstableIdCounter;
    private final AtomicLong lastApplied;
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());

    private volatile Memtable activeMemtable;
    private final Deque<Memtable> immutableMemtables;
    private final CopyOnWriteArrayList<SSTableReader> sstables;
    private volatile ManifestData manifest;
    private CompactionExecutor compactionExecutor;

    private Store(
        Path dataDirectory,
        StoreConfig config,
        LeaseProvider leaseProvider,
        Memtable activeMemtable,
        List<SSTableReader> sstables,
        ManifestData manifest,
        AtomicLong sstableIdCounter,
        long lastAppliedIndex
    ) {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.leaseProvider = leaseProvider;
        this.activeMemtable = activeMemtable;
        this.immutableMemtables = new ArrayDeque<>();
        this.sstables = new CopyOnWriteArrayList<>(sstables);
        this.manifest = manifest;
        this.sstableIdCounter = sstableIdCounter;
        this.lastApplied = new AtomicLong(lastAppliedIndex);
    }

    public static Store open(Path dataDirectory, StoreConfig config, LeaseProvider leaseProvider) {
        try {
            Files.createDirectories(dataDirectory);

            ManifestData manifest = Manifest.read(dataDirectory);
            List<SSTableReader> sstables;

            if (manifest.sstables().isEmpty()) {
                sstables = loadSSTables(dataDirectory);

                if (!sstables.isEmpty()) {
                    manifest = buildManifestFromSSTables(sstables);
                    Manifest.write(dataDirectory, manifest);
                }
            } else {
                sstables = loadSSTablesFromManifest(dataDirectory, manifest);
            }

            Memtable memtable = new SkipListMemtable(config.memtableConfig());

            Store store = new Store(
                dataDirectory,
                config,
                leaseProvider,
                memtable,
                sstables,
                manifest,
                new AtomicLong(manifest.nextSSTableId()),
                manifest.lastAppliedIndex()
            );

            LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(
                LeveledCompactionConfig.create()
            );
            store.compactionExecutor = new CompactionExecutor(store, strategy, leaseProvider);

            return store;
        } catch (IOException e) {
            throw new StoreException.RecoveryException("Failed to open store", e);
        }
    }

    @Override
    public void put(Entry entry) {
        boolean needsRotation;
        synchronized (memtableLock) {
            activeMemtable.put(entry);
            needsRotation = activeMemtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes();
        }

        if (needsRotation) {
            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException.FlushException("Interrupted waiting for flush permit", e);
            }

            synchronized (memtableLock) {
                if (activeMemtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes()) {
                    rotateMemtable();
                    flushExecutor.submit(this::flushPendingMemtables);
                } else {
                    flushPermits.release();
                }
            }
        }
    }

    @Override
    public Iterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        List<Iterator<Entry>> iterators = new ArrayList<>();

        iterators.add(activeMemtable.scan(startKey, endKey));

        synchronized (memtableLock) {
            for (Memtable immutable : immutableMemtables) {
                iterators.add(immutable.scan(startKey, endKey));
            }
        }

        for (SSTableReader sstable : sstables) {
            iterators.add(sstable.scan(startKey, endKey));
        }

        return new MergingIterator(iterators);
    }

    @Override
    public byte[] toSnapshot() {
        flush();

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer buffer = ByteBuffer.allocate(8);

            synchronized (manifestLock) {
                buffer.putLong(manifest.nextSSTableId());
                baos.write(buffer.array());
                buffer.clear();

                buffer.putInt(manifest.sstables().size());
                baos.write(buffer.array(), 0, 4);

                for (SSTableMetadata meta : manifest.sstables()) {
                    ByteBuffer metaBuffer = serializeMetadata(meta);
                    baos.write(metaBuffer.array());
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new StoreException.SnapshotException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restoreSnapshot(byte[] data) {
        try {
            for (SSTableReader sstable : sstables) {
                sstable.close();
            }
            sstables.clear();

            ByteBuffer buffer = ByteBuffer.wrap(data);

            long nextSSTableId = buffer.getLong();
            int sstableCount = buffer.getInt();

            List<SSTableMetadata> metadataList = new ArrayList<>(sstableCount);
            for (int i = 0; i < sstableCount; i++) {
                metadataList.add(deserializeMetadata(buffer));
            }

            ManifestData newManifest = new ManifestData(nextSSTableId, lastApplied.get(), metadataList);

            synchronized (manifestLock) {
                manifest = newManifest;
                Manifest.write(dataDirectory, manifest);
            }

            List<SSTableReader> newSSTables = loadSSTablesFromManifest(dataDirectory, newManifest);
            sstables.addAll(newSSTables);

            activeMemtable = new SkipListMemtable(config.memtableConfig());
            synchronized (memtableLock) {
                immutableMemtables.clear();
            }

            sstableIdCounter.set(nextSSTableId);
        } catch (Exception e) {
            throw new StoreException.SnapshotException("Failed to restore from snapshot", e);
        }
    }

    @Override
    public long lastAppliedIndex() {
        return lastApplied.get();
    }

    @Override
    public void setLastAppliedIndex(long index) {
        lastApplied.set(index);
    }

    @Override
    public void flush() {
        boolean needsRotation;
        synchronized (memtableLock) {
            needsRotation = activeMemtable.entryCount() > 0;
        }

        if (needsRotation) {
            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException.FlushException("Interrupted waiting for flush permit", e);
            }

            synchronized (memtableLock) {
                if (activeMemtable.entryCount() > 0) {
                    rotateMemtable();
                    flushExecutor.submit(this::flushPendingMemtables);
                } else {
                    flushPermits.release();
                }
            }
        }

        awaitPendingFlushes();
    }

    private void awaitPendingFlushes() {
        CompletableFuture<Void> sentinel = new CompletableFuture<>();
        flushExecutor.submit(() -> sentinel.complete(null));
        try {
            sentinel.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException.FlushException("Interrupted waiting for flush", e);
        } catch (ExecutionException e) {
            throw new StoreException.FlushException("Flush failed", e.getCause());
        }
    }

    @Override
    public void close() {
        flush();

        flushExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (compactionExecutor != null) {
            compactionExecutor.close();
        }

        for (SSTableReader sstable : sstables) {
            sstable.close();
        }
    }

    @Override
    public Optional<Entry> getEntry(ByteArray key) {
        Optional<Entry> result = activeMemtable.get(key);
        if (result.isPresent()) {
            return result;
        }

        synchronized (memtableLock) {
            for (Memtable immutable : immutableMemtables) {
                result = immutable.get(key);
                if (result.isPresent()) {
                    return result;
                }
            }
        }

        for (SSTableReader sstable : sstables) {
            result = sstable.get(key);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    private void rotateMemtable() {
        immutableMemtables.addLast(activeMemtable);
        activeMemtable = new SkipListMemtable(config.memtableConfig());
    }

    private void flushPendingMemtables() {
        while (true) {
            Memtable toFlush;
            synchronized (memtableLock) {
                toFlush = immutableMemtables.peekFirst();
                if (toFlush == null) {
                    return;
                }
            }

            try {
                flushMemtableToSSTable(toFlush);
            } finally {
                synchronized (memtableLock) {
                    immutableMemtables.pollFirst();
                }
                flushPermits.release();
            }
        }
    }

    private void flushMemtableToSSTable(Memtable memtable) {
        try {
            long sstableId = sstableIdCounter.incrementAndGet();
            Path sstablePath = dataDirectory.resolve(String.format("%06d.sst", sstableId));

            try (SSTableWriter writer = SSTableWriter.create(sstablePath, config.sstableConfig())) {
                Iterator<Entry> it = memtable.scan(null, null);
                while (it.hasNext()) {
                    writer.append(it.next());
                }
            }

            SSTableReader reader = SSTableReader.open(sstablePath);

            SSTableMetadata metadata = buildMetadata(reader, sstableId, 0);

            synchronized (manifestLock) {
                List<SSTableMetadata> updatedSSTables = new ArrayList<>(manifest.sstables());
                updatedSSTables.addFirst(metadata);
                manifest = new ManifestData(sstableId, lastApplied.get(), updatedSSTables);
                Manifest.write(dataDirectory, manifest);

                sstables.add(0, reader);
            }

            compactionExecutor.maybeScheduleCompaction();
        } catch (Exception e) {
            throw new StoreException.FlushException("Failed to flush memtable to SSTable", e);
        }
    }

    private static List<SSTableReader> loadSSTables(Path dataDirectory) throws IOException {
        List<SSTableReader> sstables = new ArrayList<>();

        if (!Files.exists(dataDirectory)) {
            return sstables;
        }

        try (Stream<Path> paths = Files.list(dataDirectory)) {
            List<Path> sstablePaths = paths
                .filter(path -> SSTABLE_PATTERN.matcher(path.getFileName().toString()).matches())
                .sorted(Comparator.comparingLong(Store::extractIdFromPath).reversed())
                .toList();

            for (Path path : sstablePaths) {
                sstables.add(SSTableReader.open(path));
            }
        }

        return sstables;
    }

    private static long extractIdFromPath(Path path) {
        Matcher matcher = SSTABLE_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Invalid SSTable filename: " + path);
    }

    private static ManifestData buildManifestFromSSTables(List<SSTableReader> sstables) throws IOException {
        List<SSTableMetadata> metadataList = new ArrayList<>();
        long maxId = 0;

        for (SSTableReader reader : sstables) {
            long id = extractIdFromPath(reader.path());
            maxId = Math.max(maxId, id);

            SSTableMetadata metadata = buildMetadata(reader, id, 0);
            metadataList.add(metadata);
        }

        return new ManifestData(maxId, 0, metadataList);
    }

    private static List<SSTableReader> loadSSTablesFromManifest(Path dataDirectory, ManifestData manifest) {
        List<SSTableMetadata> sorted = manifest.sstables().stream()
            .sorted(Comparator.comparingLong(SSTableMetadata::id).reversed())
            .toList();

        List<SSTableReader> readers = new ArrayList<>();
        for (SSTableMetadata metadata : sorted) {
            Path path = dataDirectory.resolve(String.format("%06d.sst", metadata.id()));
            readers.add(SSTableReader.open(path));
        }

        return readers;
    }

    private static SSTableMetadata buildMetadata(SSTableReader reader, long id, int level) throws IOException {
        ByteArray smallestKey = reader.index().entries().getFirst().firstKey();
        ByteArray largestKey = reader.largestKey();
        long fileSize = Files.size(reader.path());
        long entryCount = reader.entryCount();

        return new SSTableMetadata(id, level, smallestKey, largestKey, fileSize, entryCount);
    }

    private ByteBuffer serializeMetadata(SSTableMetadata meta) {
        int size = 8 + 4 + 4 + meta.smallestKey().size() + 4 + meta.largestKey().size() + 8 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putLong(meta.id());
        buffer.putInt(meta.level());
        buffer.putInt(meta.smallestKey().size());
        buffer.put(meta.smallestKey().toByteArray());
        buffer.putInt(meta.largestKey().size());
        buffer.put(meta.largestKey().toByteArray());
        buffer.putLong(meta.fileSizeBytes());
        buffer.putLong(meta.entryCount());

        return buffer;
    }

    private SSTableMetadata deserializeMetadata(ByteBuffer buffer) {
        long id = buffer.getLong();
        int level = buffer.getInt();

        int smallestKeySize = buffer.getInt();
        byte[] smallestKeyBytes = new byte[smallestKeySize];
        buffer.get(smallestKeyBytes);
        ByteArray smallestKey = ByteArray.wrap(smallestKeyBytes);

        int largestKeySize = buffer.getInt();
        byte[] largestKeyBytes = new byte[largestKeySize];
        buffer.get(largestKeyBytes);
        ByteArray largestKey = ByteArray.wrap(largestKeyBytes);

        long fileSizeBytes = buffer.getLong();
        long entryCount = buffer.getLong();

        return new SSTableMetadata(id, level, smallestKey, largestKey, fileSizeBytes, entryCount);
    }

    public ManifestData getManifest() {
        synchronized (manifestLock) {
            return manifest;
        }
    }

    public void swapSSTables(List<SSTableMetadata> oldMeta, List<SSTableMetadata> newMeta) {
        synchronized (manifestLock) {
            List<SSTableMetadata> updated = new ArrayList<>(manifest.sstables());
            updated.removeAll(oldMeta);
            updated.addAll(newMeta);
            manifest = new ManifestData(manifest.nextSSTableId(), lastApplied.get(), updated);
            Manifest.write(dataDirectory, manifest);

            sstables.removeIf(r -> oldMeta.stream()
                .anyMatch(m -> extractIdFromPath(r.path()) == m.id()));

            for (SSTableMetadata meta : newMeta) {
                Path path = dataDirectory.resolve(String.format("%06d.sst", meta.id()));
                sstables.add(SSTableReader.open(path));
            }
        }
    }

    public long nextSSTableId() {
        synchronized (manifestLock) {
            long nextId = sstableIdCounter.incrementAndGet();
            manifest = new ManifestData(nextId, lastApplied.get(), manifest.sstables());
            return nextId;
        }
    }

    public Path sstablePath(long id) {
        return dataDirectory.resolve(String.format("%06d.sst", id));
    }

    public SSTableConfig sstableConfig() {
        return config.sstableConfig();
    }
}
