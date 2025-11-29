package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.KeyValue;
import io.partdb.storage.compaction.CompactionExecutor;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.compaction.LeveledCompactionStrategy;
import io.partdb.storage.compaction.Manifest;
import io.partdb.storage.compaction.ManifestData;
import io.partdb.storage.compaction.SSTableMetadata;
import io.partdb.storage.SSTableSnapshot.AcquireResult;
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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Store implements KeyValueStore {

    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final int MAX_IMMUTABLE_MEMTABLES = 4;
    private static final int MAX_SNAPSHOT_ACQUIRE_ATTEMPTS = 100;
    private static final long INITIAL_BACKOFF_NANOS = 100;
    private static final long MAX_BACKOFF_NANOS = 10_000;
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    private final Path dataDirectory;
    private final StoreConfig config;
    private final AtomicReference<Memtable> activeMemtable;
    private final Deque<Memtable> immutableMemtables;
    private final StampedLock memtableLock;
    private final ReentrantLock rotationLock;
    private final ReentrantLock manifestLock;
    private final Semaphore flushPermits;
    private final AtomicLong sstableIdCounter;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean closed;

    private volatile SSTableSnapshot sstableSnapshot;
    private volatile ManifestData manifest;
    private CompactionExecutor compactionExecutor;

    private Store(
        Path dataDirectory,
        StoreConfig config,
        Memtable activeMemtable,
        SSTableSnapshot sstableSnapshot,
        ManifestData manifest,
        AtomicLong sstableIdCounter
    ) {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.activeMemtable = new AtomicReference<>(activeMemtable);
        this.immutableMemtables = new ArrayDeque<>();
        this.memtableLock = new StampedLock();
        this.rotationLock = new ReentrantLock();
        this.manifestLock = new ReentrantLock();
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.sstableIdCounter = sstableIdCounter;
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.closed = new AtomicBoolean(false);
        this.sstableSnapshot = sstableSnapshot;
        this.manifest = manifest;
    }

    public static Store open(Path dataDirectory, StoreConfig config) {
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
                memtable,
                SSTableSnapshot.of(sstables),
                manifest,
                new AtomicLong(manifest.nextSSTableId())
            );

            LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(
                LeveledCompactionConfig.create()
            );
            store.compactionExecutor = new CompactionExecutor(store, strategy);

            return store;
        } catch (IOException e) {
            throw new StoreException.RecoveryException("Failed to open store", e);
        }
    }

    @Override
    public void put(ByteArray key, ByteArray value) {
        StoreEntry entry = StoreEntry.of(key, value);
        putEntry(entry);
    }

    @Override
    public void delete(ByteArray key) {
        StoreEntry entry = StoreEntry.tombstone(key);
        putEntry(entry);
    }

    private void putEntry(StoreEntry entry) {
        Memtable memtable = activeMemtable.get();
        memtable.put(entry);

        if (memtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes()) {
            maybeRotateMemtable(memtable);
        }
    }

    private void maybeRotateMemtable(Memtable fullMemtable) {
        if (!rotationLock.tryLock()) {
            return;
        }
        try {
            Memtable current = activeMemtable.get();
            if (current != fullMemtable) {
                return;
            }
            if (current.sizeInBytes() < config.memtableConfig().maxSizeInBytes()) {
                return;
            }

            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException.FlushException("Interrupted waiting for flush permit", e);
            }

            Memtable newMemtable = new SkipListMemtable(config.memtableConfig());
            long stamp = memtableLock.writeLock();
            try {
                immutableMemtables.addLast(current);
                activeMemtable.set(newMemtable);
            } finally {
                memtableLock.unlockWrite(stamp);
            }

            flushExecutor.submit(this::flushPendingMemtables);
        } finally {
            rotationLock.unlock();
        }
    }

    @Override
    public Optional<ByteArray> get(ByteArray key) {
        Optional<StoreEntry> result = activeMemtable.get().get(key);
        if (result.isPresent()) {
            return entryToValue(result.get());
        }

        long stamp = memtableLock.tryOptimisticRead();
        List<Memtable> immutablesCopy = List.copyOf(immutableMemtables);
        if (!memtableLock.validate(stamp)) {
            stamp = memtableLock.readLock();
            try {
                immutablesCopy = List.copyOf(immutableMemtables);
            } finally {
                memtableLock.unlockRead(stamp);
            }
        }

        for (Memtable immutable : immutablesCopy) {
            result = immutable.get(key);
            if (result.isPresent()) {
                return entryToValue(result.get());
            }
        }

        SSTableSnapshot snapshot = acquireSnapshot();
        try {
            for (SSTableReader sstable : snapshot.readers()) {
                result = sstable.get(key);
                if (result.isPresent()) {
                    return entryToValue(result.get());
                }
            }
            return Optional.empty();
        } finally {
            snapshot.release();
        }
    }

    private Optional<ByteArray> entryToValue(StoreEntry entry) {
        if (entry.tombstone()) {
            return Optional.empty();
        }
        return Optional.of(entry.value());
    }

    @Override
    public CloseableIterator<KeyValue> scan(ByteArray startKey, ByteArray endKey) {
        List<CloseableIterator<StoreEntry>> iterators = new ArrayList<>();

        iterators.add(activeMemtable.get().scan(startKey, endKey));

        long stamp = memtableLock.tryOptimisticRead();
        List<Memtable> immutablesCopy = List.copyOf(immutableMemtables);
        if (!memtableLock.validate(stamp)) {
            stamp = memtableLock.readLock();
            try {
                immutablesCopy = List.copyOf(immutableMemtables);
            } finally {
                memtableLock.unlockRead(stamp);
            }
        }

        for (Memtable immutable : immutablesCopy) {
            iterators.add(immutable.scan(startKey, endKey));
        }

        SSTableSnapshot snapshot = acquireSnapshot();
        for (SSTableReader sstable : snapshot.readers()) {
            iterators.add(sstable.scan(startKey, endKey));
        }

        MergingIterator merged = new MergingIterator(iterators);
        return new KeyValueIterator(merged, snapshot);
    }

    private SSTableSnapshot acquireSnapshot() {
        long backoffNanos = INITIAL_BACKOFF_NANOS;

        for (int attempt = 0; attempt < MAX_SNAPSHOT_ACQUIRE_ATTEMPTS; attempt++) {
            SSTableSnapshot current = sstableSnapshot;
            AcquireResult result = current.tryAcquire();

            switch (result) {
                case AcquireResult.Success(var snapshot) -> {
                    return snapshot;
                }
                case AcquireResult.Retired _ -> {
                    if (attempt < 16) {
                        Thread.onSpinWait();
                    } else {
                        LockSupport.parkNanos(backoffNanos);
                        backoffNanos = Math.min(backoffNanos * 2, MAX_BACKOFF_NANOS);
                    }
                }
            }
        }

        throw new StoreException.ConcurrencyException(
            "Failed to acquire SSTable snapshot after " + MAX_SNAPSHOT_ACQUIRE_ATTEMPTS + " attempts"
        );
    }

    @Override
    public byte[] snapshot() {
        flush();

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(8);

            manifestLock.lock();
            try {
                header.putLong(manifest.nextSSTableId());
                baos.write(header.array());

                ByteBuffer countBuffer = ByteBuffer.allocate(4);
                countBuffer.putInt(manifest.sstables().size());
                baos.write(countBuffer.array());

                for (SSTableMetadata meta : manifest.sstables()) {
                    ByteBuffer metaBuffer = ByteBuffer.allocate(meta.serializedSize());
                    meta.writeTo(metaBuffer);
                    baos.write(metaBuffer.array());
                }
            } finally {
                manifestLock.unlock();
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new StoreException.SnapshotException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restore(byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            long nextSSTableId = buffer.getLong();
            int sstableCount = buffer.getInt();

            List<SSTableMetadata> metadataList = new ArrayList<>(sstableCount);
            for (int i = 0; i < sstableCount; i++) {
                metadataList.add(SSTableMetadata.readFrom(buffer));
            }

            ManifestData newManifest = new ManifestData(nextSSTableId, metadataList);
            List<SSTableReader> newReaders = loadSSTablesFromManifest(dataDirectory, newManifest);

            manifestLock.lock();
            try {
                manifest = newManifest;
                Manifest.write(dataDirectory, manifest);

                SSTableSnapshot oldSnapshot = sstableSnapshot;
                List<SSTableReader> orphanedReaders = new ArrayList<>(oldSnapshot.readers());
                sstableSnapshot = SSTableSnapshot.of(newReaders);
                oldSnapshot.retire(orphanedReaders);
            } finally {
                manifestLock.unlock();
            }

            Memtable newMemtable = new SkipListMemtable(config.memtableConfig());
            long stamp = memtableLock.writeLock();
            try {
                activeMemtable.set(newMemtable);
                immutableMemtables.clear();
            } finally {
                memtableLock.unlockWrite(stamp);
            }

            sstableIdCounter.set(nextSSTableId);
        } catch (Exception e) {
            throw new StoreException.SnapshotException("Failed to restore from snapshot", e);
        }
    }

    @Override
    public void flush() {
        rotationLock.lock();
        try {
            Memtable current = activeMemtable.get();
            if (current.entryCount() == 0) {
                awaitPendingFlushes();
                return;
            }

            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException.FlushException("Interrupted waiting for flush permit", e);
            }

            Memtable newMemtable = new SkipListMemtable(config.memtableConfig());
            long stamp = memtableLock.writeLock();
            try {
                immutableMemtables.addLast(current);
                activeMemtable.set(newMemtable);
            } finally {
                memtableLock.unlockWrite(stamp);
            }

            flushExecutor.submit(this::flushPendingMemtables);
        } finally {
            rotationLock.unlock();
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        flush();

        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flushExecutor.shutdownNow();
        }

        if (compactionExecutor != null) {
            compactionExecutor.close();
        }

        SSTableSnapshot finalSnapshot = sstableSnapshot;
        List<SSTableReader> allReaders = new ArrayList<>(finalSnapshot.readers());
        finalSnapshot.retire(allReaders);

        awaitSnapshotDrain(finalSnapshot, SHUTDOWN_DRAIN_TIMEOUT);
    }

    private void awaitSnapshotDrain(SSTableSnapshot snapshot, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (!snapshot.isDrained()) {
            if (System.nanoTime() > deadlineNanos) {
                for (SSTableReader reader : snapshot.readers()) {
                    reader.close();
                }
                return;
            }
            LockSupport.parkNanos(1_000_000);
        }
    }

    private void flushPendingMemtables() {
        while (true) {
            Memtable toFlush;
            long stamp = memtableLock.readLock();
            try {
                toFlush = immutableMemtables.peekFirst();
                if (toFlush == null) {
                    return;
                }
            } finally {
                memtableLock.unlockRead(stamp);
            }

            try {
                flushMemtableToSSTable(toFlush);
            } finally {
                stamp = memtableLock.writeLock();
                try {
                    immutableMemtables.pollFirst();
                } finally {
                    memtableLock.unlockWrite(stamp);
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
                Iterator<StoreEntry> it = memtable.scan(null, null);
                while (it.hasNext()) {
                    writer.append(it.next());
                }
            }

            SSTableReader reader = SSTableReader.open(sstablePath);
            SSTableMetadata metadata = buildMetadata(reader, sstableId, 0);

            manifestLock.lock();
            try {
                List<SSTableMetadata> updatedSSTables = new ArrayList<>(manifest.sstables());
                updatedSSTables.addFirst(metadata);
                manifest = new ManifestData(sstableId, updatedSSTables);
                Manifest.write(dataDirectory, manifest);

                List<SSTableReader> newReaders = new ArrayList<>();
                newReaders.add(reader);
                newReaders.addAll(sstableSnapshot.readers());

                SSTableSnapshot oldSnapshot = sstableSnapshot;
                sstableSnapshot = SSTableSnapshot.of(newReaders);
                oldSnapshot.retire(List.of());
            } finally {
                manifestLock.unlock();
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

        return new ManifestData(maxId, metadataList);
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

    public ManifestData getManifest() {
        manifestLock.lock();
        try {
            return manifest;
        } finally {
            manifestLock.unlock();
        }
    }

    public void swapSSTables(List<SSTableMetadata> oldMeta, List<SSTableMetadata> newMeta) {
        manifestLock.lock();
        try {
            List<SSTableMetadata> updated = new ArrayList<>(manifest.sstables());
            updated.removeAll(oldMeta);
            updated.addAll(newMeta);
            manifest = new ManifestData(manifest.nextSSTableId(), updated);
            Manifest.write(dataDirectory, manifest);

            Set<Long> oldIds = oldMeta.stream()
                .map(SSTableMetadata::id)
                .collect(Collectors.toSet());

            List<SSTableReader> orphanedReaders = new ArrayList<>();
            List<SSTableReader> retainedReaders = new ArrayList<>();

            for (SSTableReader reader : sstableSnapshot.readers()) {
                if (oldIds.contains(extractIdFromPath(reader.path()))) {
                    orphanedReaders.add(reader);
                } else {
                    retainedReaders.add(reader);
                }
            }

            for (SSTableMetadata meta : newMeta) {
                Path path = dataDirectory.resolve(String.format("%06d.sst", meta.id()));
                retainedReaders.add(SSTableReader.open(path));
            }

            SSTableSnapshot oldSnapshot = sstableSnapshot;
            sstableSnapshot = SSTableSnapshot.of(retainedReaders);
            oldSnapshot.retire(orphanedReaders);
        } finally {
            manifestLock.unlock();
        }
    }

    public long nextSSTableId() {
        manifestLock.lock();
        try {
            long nextId = sstableIdCounter.incrementAndGet();
            manifest = new ManifestData(nextId, manifest.sstables());
            return nextId;
        } finally {
            manifestLock.unlock();
        }
    }

    public Path sstablePath(long id) {
        return dataDirectory.resolve(String.format("%06d.sst", id));
    }

    public SSTableConfig sstableConfig() {
        return config.sstableConfig();
    }

    private static final class KeyValueIterator implements CloseableIterator<KeyValue> {

        private final MergingIterator delegate;
        private final SSTableSnapshot snapshot;
        private KeyValue next;

        KeyValueIterator(MergingIterator delegate, SSTableSnapshot snapshot) {
            this.delegate = delegate;
            this.snapshot = snapshot;
            advance();
        }

        private void advance() {
            while (delegate.hasNext()) {
                StoreEntry entry = delegate.next();
                if (!entry.tombstone()) {
                    next = new KeyValue(entry.key(), entry.value());
                    return;
                }
            }
            next = null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public KeyValue next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            KeyValue result = next;
            advance();
            return result;
        }

        @Override
        public void close() {
            delegate.close();
            snapshot.release();
        }
    }
}
