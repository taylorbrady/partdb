package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public final class StorageEngine implements AutoCloseable {

    private final LsmConfig lsmConfig;
    private final AtomicReference<MutableMemtable> activeMemtable;
    private final ReentrantLock rotationLock;
    private final AtomicBoolean closed;
    private final SSTableCatalog sstableCatalog;
    private final FlushCoordinator flushCoordinator;
    private final StorageRuntimeStats runtimeStats;
    private final ReadCoordinator readCoordinator;
    private final CheckpointService checkpointService;

    private StorageEngine(LsmConfig lsmConfig, SSTableCatalog sstableCatalog, StorageRuntimeStats runtimeStats) {
        this.lsmConfig = Objects.requireNonNull(lsmConfig, "lsmConfig must not be null");
        this.sstableCatalog = Objects.requireNonNull(sstableCatalog, "sstableCatalog must not be null");
        this.runtimeStats = Objects.requireNonNull(runtimeStats, "runtimeStats must not be null");
        this.activeMemtable = new AtomicReference<>(new MutableMemtable());
        this.rotationLock = new ReentrantLock();
        this.closed = new AtomicBoolean(false);
        this.flushCoordinator = new FlushCoordinator(sstableCatalog, this::refreshMemtableStats);
        this.readCoordinator = new ReadCoordinator(activeMemtable::get, flushCoordinator::immutableMemtables, sstableCatalog);
        this.checkpointService = new CheckpointService(
            sstableCatalog,
            flushCoordinator,
            rotationLock,
            this::resetMemtables,
            this::flush,
            runtimeStats
        );
        refreshMemtableStats();
    }

    public static StorageEngine open(Path dataDirectory) {
        return open(dataDirectory, StorageOptions.defaults());
    }

    public static StorageEngine open(Path dataDirectory, StorageOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        return open(dataDirectory, options.toLsmConfig());
    }

    static StorageEngine open(Path dataDirectory, LsmConfig config) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        try {
            Files.createDirectories(dataDirectory);
            StorageRuntimeStats runtimeStats = new StorageRuntimeStats();
            SSTableCatalog sstableCatalog = SSTableCatalog.open(dataDirectory, config, runtimeStats);
            return new StorageEngine(config, sstableCatalog, runtimeStats);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open store", e);
        }
    }

    public static StorageEngine restore(Path dataDirectory, StorageCheckpoint checkpoint, StorageOptions options) {
        StorageEngine engine = open(dataDirectory, options);
        try {
            engine.restoreInPlace(checkpoint);
            return engine;
        } catch (RuntimeException e) {
            engine.close();
            throw e;
        }
    }

    public void apply(Revision revision, Mutation mutation) {
        apply(revision, WriteBatch.of(mutation));
    }

    public void apply(Revision revision, WriteBatch batch) {
        Objects.requireNonNull(revision, "revision must not be null");
        Objects.requireNonNull(batch, "batch must not be null");
        if (batch.isEmpty()) {
            return;
        }

        List<StoredEntry> entries = new ArrayList<>(batch.mutations().size());
        for (Mutation mutation : batch.mutations()) {
            entries.add(toStoredEntry(revision, mutation));
        }
        apply(entries);
    }

    void apply(List<StoredEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        rotationLock.lock();
        try {
            validateBatch(entries);
            for (StoredEntry entry : entries) {
                appendValidatedEntry(entry);
            }
            long maxRevision = entries.stream()
                .mapToLong(StoredEntry::revision)
                .max()
                .orElse(0);
            sstableCatalog.updateAppliedThrough(maxRevision);
        } finally {
            rotationLock.unlock();
        }
    }

    public Optional<ValueRecord> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        return get(copy(key))
            .map(entry -> new ValueRecord(Bytes.copyOf(entry.value().toByteArray()), new Revision(entry.revision())));
    }

    Optional<StoredEntry.Value> get(Slice key) {
        return readCoordinator.get(key);
    }

    public Scan scan(KeyRange range) {
        Objects.requireNonNull(range, "range must not be null");
        return new StorageScan(scan(toScanBounds(range)));
    }

    CloseableIterator<StoredEntry.Value> scan(ScanBounds bounds) {
        return readCoordinator.scan(bounds);
    }

    public StorageCheckpoint checkpoint() {
        return new StorageCheckpoint(Bytes.copyOf(checkpointBytes()));
    }

    byte[] checkpointBytes() {
        return checkpointService.checkpoint();
    }

    public void restoreInPlace(StorageCheckpoint checkpoint) {
        replaceWithCheckpoint(Objects.requireNonNull(checkpoint, "checkpoint must not be null").bytes().toByteArray());
    }

    void replaceWithCheckpoint(byte[] data) {
        checkpointService.restore(data);
    }

    public StorageMetadata metadata() {
        return new StorageMetadata(new Revision(sstableCatalog.appliedThroughRevision()));
    }

    public LsmStats stats() {
        refreshMemtableStats();
        return runtimeStats.snapshot();
    }

    SSTableManifest manifest() {
        return sstableCatalog.manifest();
    }

    void awaitCompactionIdle(Duration timeout) {
        sstableCatalog.awaitCompactionIdle(timeout);
    }

    void flush() {
        rotationLock.lock();
        try {
            MutableMemtable current = activeMemtable.get();
            if (current.entryCount() == 0) {
                flushCoordinator.awaitIdle();
                return;
            }
            rotateMemtable(current);
        } finally {
            rotationLock.unlock();
        }

        flushCoordinator.awaitIdle();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        flush();
        flushCoordinator.close();
        sstableCatalog.close();
    }

    private void appendValidatedEntry(StoredEntry entry) {
        while (true) {
            MutableMemtable memtable = activeMemtable.get();
            MutableMemtable.WriteResult writeResult = memtable.put(entry);
            if (writeResult == MutableMemtable.WriteResult.DUPLICATE) {
                return;
            }
            if (writeResult == MutableMemtable.WriteResult.FROZEN) {
                continue;
            }

            refreshMemtableStats();

            if (memtable.sizeInBytes() >= lsmConfig.memtableMaxSizeBytes()) {
                tryRotateMemtable(memtable);
            }
            return;
        }
    }

    private void validateBatch(List<StoredEntry> entries) {
        Set<Slice> keys = new java.util.HashSet<>(entries.size());
        for (StoredEntry entry : entries) {
            if (!keys.add(entry.key())) {
                throw new IllegalArgumentException("Write batch contains duplicate key: " + entry.key());
            }
            validateStoredEntry(entry);
        }
    }

    private void tryRotateMemtable(MutableMemtable fullMemtable) {
        if (!rotationLock.tryLock()) {
            return;
        }
        try {
            MutableMemtable current = activeMemtable.get();
            if (current != fullMemtable) {
                return;
            }
            if (current.sizeInBytes() < lsmConfig.memtableMaxSizeBytes()) {
                return;
            }
            rotateMemtable(current);
        } finally {
            rotationLock.unlock();
        }
    }

    private void rotateMemtable(MutableMemtable current) {
        flushCoordinator.enqueue(current.freeze());
        activeMemtable.set(new MutableMemtable());
        refreshMemtableStats();
    }

    private void resetMemtables() {
        activeMemtable.set(new MutableMemtable());
        refreshMemtableStats();
    }

    private void validateStoredEntry(StoredEntry entry) {
        Optional<StoredEntry> existing = readCoordinator.lookupLatestEntry(entry.key());
        if (existing.isEmpty()) {
            return;
        }

        StoredEntry current = existing.get();
        if (entry.revision() > current.revision()) {
            return;
        }

        if (entry.revision() == current.revision() && entry.equals(current)) {
            return;
        }

        if (entry.revision() < current.revision()) {
            throw new StorageException.InvalidRevision(
                "Revision %d for key %s is older than current revision %d"
                    .formatted(entry.revision(), entry.key(), current.revision())
            );
        }

        throw new StorageException.InvalidRevision(
            "Conflicting mutation for key %s at revision %d"
                .formatted(entry.key(), entry.revision())
        );
    }

    private void refreshMemtableStats() {
        runtimeStats.updateMemtables(activeMemtable.get().sizeInBytes(), flushCoordinator.immutableMemtables().size());
    }

    private static Slice copy(Bytes bytes) {
        return Slice.copyOf(bytes.toByteArray());
    }

    private static StoredEntry toStoredEntry(Revision revision, Mutation mutation) {
        return switch (mutation) {
            case Mutation.Put(var key, var value) -> new StoredEntry.Value(copy(key), copy(value), revision.value());
            case Mutation.Delete(var key) -> new StoredEntry.Tombstone(copy(key), revision.value());
        };
    }

    private static ScanBounds toScanBounds(KeyRange range) {
        return switch (range) {
            case KeyRange.All _ -> ScanBounds.all();
            case KeyRange.From(var startInclusive) -> ScanBounds.from(copy(startInclusive));
            case KeyRange.Until(var endExclusive) -> ScanBounds.until(copy(endExclusive));
            case KeyRange.Between(var startInclusive, var endExclusive) ->
                ScanBounds.between(copy(startInclusive), copy(endExclusive));
        };
    }

    private static EntryRecord toEntryRecord(StoredEntry.Value entry) {
        return new EntryRecord(
            Bytes.copyOf(entry.key().toByteArray()),
            Bytes.copyOf(entry.value().toByteArray()),
            new Revision(entry.revision())
        );
    }

    private static final class StorageScan implements Scan {
        private final CloseableIterator<StoredEntry.Value> cursor;
        private boolean iteratorIssued;

        private StorageScan(CloseableIterator<StoredEntry.Value> cursor) {
            this.cursor = Objects.requireNonNull(cursor, "cursor must not be null");
            this.iteratorIssued = false;
        }

        @Override
        public Iterator<EntryRecord> iterator() {
            if (iteratorIssued) {
                throw new IllegalStateException("Scan supports only a single iterator");
            }
            iteratorIssued = true;
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return cursor.hasNext();
                }

                @Override
                public EntryRecord next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return toEntryRecord(cursor.next());
                }
            };
        }

        @Override
        public void close() {
            cursor.close();
        }
    }
}
