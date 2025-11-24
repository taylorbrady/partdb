package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.Lease;
import io.partdb.common.statemachine.*;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class Store implements StateMachine, AutoCloseable {

    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");

    private final Path dataDirectory;
    private final StoreConfig config;
    private final Object memtableLock = new Object();
    private final Object manifestLock = new Object();
    private final Object leaseLock = new Object();
    private final AtomicLong sstableIdCounter;
    private final AtomicLong lastApplied;

    private volatile Memtable activeMemtable;
    private final Deque<Memtable> immutableMemtables;
    private final CopyOnWriteArrayList<SSTableReader> sstables;
    private volatile ManifestData manifest;
    private CompactionExecutor compactionExecutor;

    private final Map<Long, Lease> activeLeases;
    private final Map<Long, Set<ByteArray>> leaseToKeys;
    private final Map<ByteArray, Long> keyToLease;
    private final Set<Long> revokedLeases;

    private Store(
        Path dataDirectory,
        StoreConfig config,
        Memtable activeMemtable,
        List<SSTableReader> sstables,
        ManifestData manifest,
        AtomicLong sstableIdCounter,
        long lastAppliedIndex
    ) {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.activeMemtable = activeMemtable;
        this.immutableMemtables = new ArrayDeque<>();
        this.sstables = new CopyOnWriteArrayList<>(sstables);
        this.manifest = manifest;
        this.sstableIdCounter = sstableIdCounter;
        this.lastApplied = new AtomicLong(lastAppliedIndex);
        this.activeLeases = new ConcurrentHashMap<>();
        this.leaseToKeys = new ConcurrentHashMap<>();
        this.keyToLease = new ConcurrentHashMap<>();
        this.revokedLeases = ConcurrentHashMap.newKeySet();
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

            Store engine = new Store(
                dataDirectory,
                config,
                memtable,
                sstables,
                manifest,
                new AtomicLong(manifest.nextSSTableId()),
                manifest.lastAppliedIndex()
            );

            LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(
                LeveledCompactionConfig.create()
            );
            engine.compactionExecutor = new CompactionExecutor(engine, strategy);

            return engine;
        } catch (IOException e) {
            throw new StoreException.RecoveryException("Failed to open store", e);
        }
    }

    @Override
    public void apply(long index, Operation operation) {
        lastApplied.set(index);

        switch (operation) {
            case Put put -> {
                Entry entry = Entry.putWithLease(put.key(), put.value(), index, put.leaseId());

                synchronized (memtableLock) {
                    activeMemtable.put(entry);

                    if (activeMemtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes()) {
                        rotateMemtable();
                    }
                }

                if (put.leaseId() != 0) {
                    synchronized (leaseLock) {
                        leaseToKeys.computeIfAbsent(put.leaseId(), k -> ConcurrentHashMap.newKeySet()).add(put.key());
                        keyToLease.put(put.key(), put.leaseId());
                    }
                }

                flushImmutableMemtables();
            }
            case Delete delete -> {
                Entry entry = Entry.delete(delete.key(), index);

                synchronized (memtableLock) {
                    activeMemtable.put(entry);

                    if (activeMemtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes()) {
                        rotateMemtable();
                    }
                }

                synchronized (leaseLock) {
                    Long leaseId = keyToLease.remove(delete.key());
                    if (leaseId != null) {
                        Set<ByteArray> keys = leaseToKeys.get(leaseId);
                        if (keys != null) {
                            keys.remove(delete.key());
                        }
                    }
                }

                flushImmutableMemtables();
            }
            case GrantLease grantLease -> {
                Lease lease = new Lease(grantLease.leaseId(), grantLease.ttlMillis(), grantLease.grantedAtMillis());
                synchronized (leaseLock) {
                    activeLeases.put(grantLease.leaseId(), lease);
                    revokedLeases.remove(grantLease.leaseId());
                }
            }
            case RevokeLease revokeLease -> {
                synchronized (leaseLock) {
                    activeLeases.remove(revokeLease.leaseId());
                    revokedLeases.add(revokeLease.leaseId());
                }
            }
            case KeepAliveLease keepAliveLease -> {
                synchronized (leaseLock) {
                    Lease existing = activeLeases.get(keepAliveLease.leaseId());
                    if (existing != null) {
                        Lease renewed = existing.renew(System.currentTimeMillis());
                        activeLeases.put(keepAliveLease.leaseId(), renewed);
                    }
                }
            }
        }
    }

    @Override
    public Optional<ByteArray> get(ByteArray key) {
        Optional<Entry> result = getEntry(key);
        return result.map(Entry::value);
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
    public StateSnapshot snapshot() {
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

            synchronized (leaseLock) {
                buffer.clear();
                buffer.putInt(activeLeases.size());
                baos.write(buffer.array(), 0, 4);

                for (Lease lease : activeLeases.values()) {
                    buffer.clear();
                    buffer.putLong(lease.id());
                    baos.write(buffer.array());
                    buffer.clear();
                    buffer.putLong(lease.ttlMillis());
                    baos.write(buffer.array());
                    buffer.clear();
                    buffer.putLong(lease.grantedAtMillis());
                    baos.write(buffer.array());
                }

                buffer.clear();
                buffer.putInt(revokedLeases.size());
                baos.write(buffer.array(), 0, 4);

                for (Long revokedLeaseId : revokedLeases) {
                    buffer.clear();
                    buffer.putLong(revokedLeaseId);
                    baos.write(buffer.array());
                }
            }

            byte[] data = baos.toByteArray();
            return StateSnapshot.create(lastApplied.get(), data);
        } catch (IOException e) {
            throw new StoreException.SnapshotException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restore(StateSnapshot snapshot) {
        try {
            close();

            ByteBuffer buffer = ByteBuffer.wrap(snapshot.data());

            long nextSSTableId = buffer.getLong();
            int sstableCount = buffer.getInt();

            List<SSTableMetadata> metadataList = new ArrayList<>(sstableCount);
            for (int i = 0; i < sstableCount; i++) {
                metadataList.add(deserializeMetadata(buffer));
            }

            int activeLeaseCount = buffer.getInt();
            Map<Long, Lease> restoredLeases = new HashMap<>(activeLeaseCount);
            for (int i = 0; i < activeLeaseCount; i++) {
                long leaseId = buffer.getLong();
                long ttlMillis = buffer.getLong();
                long grantedAtMillis = buffer.getLong();
                restoredLeases.put(leaseId, new Lease(leaseId, ttlMillis, grantedAtMillis));
            }

            int revokedLeaseCount = buffer.getInt();
            Set<Long> restoredRevokedLeases = new HashSet<>(revokedLeaseCount);
            for (int i = 0; i < revokedLeaseCount; i++) {
                restoredRevokedLeases.add(buffer.getLong());
            }

            ManifestData newManifest = new ManifestData(nextSSTableId, snapshot.lastAppliedIndex(), metadataList);

            synchronized (manifestLock) {
                manifest = newManifest;
                Manifest.write(dataDirectory, manifest);
            }

            List<SSTableReader> newSSTables = loadSSTablesFromManifest(dataDirectory, newManifest);
            sstables.clear();
            sstables.addAll(newSSTables);

            activeMemtable = new SkipListMemtable(config.memtableConfig());
            immutableMemtables.clear();

            synchronized (leaseLock) {
                activeLeases.clear();
                activeLeases.putAll(restoredLeases);
                leaseToKeys.clear();
                keyToLease.clear();
                revokedLeases.clear();
                revokedLeases.addAll(restoredRevokedLeases);
            }

            lastApplied.set(snapshot.lastAppliedIndex());
            sstableIdCounter.set(nextSSTableId);

            LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(
                LeveledCompactionConfig.create()
            );
            compactionExecutor = new CompactionExecutor(this, strategy);
        } catch (Exception e) {
            throw new StoreException.SnapshotException("Failed to restore from snapshot", e);
        }
    }

    @Override
    public long lastAppliedIndex() {
        return lastApplied.get();
    }

    public void flush() {
        synchronized (memtableLock) {
            if (activeMemtable.entryCount() > 0) {
                rotateMemtable();
            }
        }

        flushImmutableMemtables();
    }

    public List<Long> getExpiredLeases(long currentTimeMillis) {
        List<Long> expired = new ArrayList<>();
        synchronized (leaseLock) {
            for (Map.Entry<Long, Lease> entry : activeLeases.entrySet()) {
                if (entry.getValue().isExpired(currentTimeMillis)) {
                    expired.add(entry.getKey());
                }
            }
        }
        return expired;
    }

    @Override
    public void close() {
        flush();

        if (compactionExecutor != null) {
            compactionExecutor.close();
        }

        for (SSTableReader sstable : sstables) {
            sstable.close();
        }
    }

    private Optional<Entry> getEntry(ByteArray key) {
        Optional<Entry> result = activeMemtable.get(key);
        if (result.isPresent()) {
            return handleEntry(result.get());
        }

        synchronized (memtableLock) {
            for (Memtable immutable : immutableMemtables) {
                result = immutable.get(key);
                if (result.isPresent()) {
                    return handleEntry(result.get());
                }
            }
        }

        for (SSTableReader sstable : sstables) {
            result = sstable.get(key);
            if (result.isPresent()) {
                return handleEntry(result.get());
            }
        }

        return Optional.empty();
    }

    private void rotateMemtable() {
        immutableMemtables.addLast(activeMemtable);
        activeMemtable = new SkipListMemtable(config.memtableConfig());
    }

    private void flushImmutableMemtables() {
        while (true) {
            Memtable toFlush;
            synchronized (memtableLock) {
                toFlush = immutableMemtables.pollFirst();
                if (toFlush == null) {
                    return;
                }
            }

            flushMemtableToSSTable(toFlush);
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

    private Optional<Entry> handleEntry(Entry entry) {
        if (entry.tombstone()) {
            return Optional.empty();
        }
        if (entry.leaseId() != 0 && revokedLeases.contains(entry.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(entry);
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
