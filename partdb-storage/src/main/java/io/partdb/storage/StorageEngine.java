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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public final class StorageEngine implements AutoCloseable {

    private final LsmConfig lsmConfig;
    private final MemtableSet memtables;
    private final ReentrantLock rotationLock;
    private final AtomicBoolean closed;
    private final SstableStore sstableStore;
    private final VersionSet versionSet;
    private final CompactionScheduler compactionScheduler;
    private final AtomicLong appliedThroughRevision;
    private final MemtableFlusher memtableFlusher;
    private final StorageRuntimeStats runtimeStats;
    private final Checkpointer checkpointer;

    private StorageEngine(
        LsmConfig lsmConfig,
        SstableStore sstableStore,
        VersionSet versionSet,
        CompactionScheduler compactionScheduler,
        long appliedThroughRevision,
        CheckpointInstaller checkpointInstaller,
        StorageRuntimeStats runtimeStats
    ) {
        this.lsmConfig = Objects.requireNonNull(lsmConfig, "lsmConfig must not be null");
        this.sstableStore = Objects.requireNonNull(sstableStore, "sstableStore must not be null");
        this.versionSet = Objects.requireNonNull(versionSet, "versionSet must not be null");
        this.compactionScheduler = Objects.requireNonNull(compactionScheduler, "compactionScheduler must not be null");
        this.appliedThroughRevision = new AtomicLong(appliedThroughRevision);
        this.runtimeStats = Objects.requireNonNull(runtimeStats, "runtimeStats must not be null");
        this.memtables = new MemtableSet();
        this.rotationLock = new ReentrantLock();
        this.closed = new AtomicBoolean(false);
        this.memtableFlusher = new MemtableFlusher(
            sstableStore,
            versionSet,
            memtables,
            this.appliedThroughRevision::get,
            this::refreshMemtableStats,
            compactionScheduler::requestCompaction
        );
        this.checkpointer = new Checkpointer(
            versionSet,
            checkpointInstaller,
            compactionScheduler,
            memtableFlusher,
            rotationLock,
            this::resetMemtables,
            this::flush,
            this.appliedThroughRevision::get,
            this.appliedThroughRevision::set,
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
            BlockCache blockCache = config.cacheEnabled()
                ? new S3FifoBlockCache(config.blockCacheMaxBytes())
                : NoOpBlockCache.INSTANCE;
            ManifestStore manifestStore = new ManifestStore(dataDirectory);
            SstableStore sstableStore = new SstableStore(dataDirectory, config, blockCache);
            CheckpointInstaller checkpointInstaller = new CheckpointInstaller(dataDirectory, manifestStore, sstableStore);
            LoadedStoreVersion initialState = sstableStore.openState(manifestStore);
            VersionSet versionSet = VersionSet.open(manifestStore, initialState, runtimeStats);
            Compactor compactor = new Compactor(sstableStore, versionSet, config);
            CompactionScheduler compactionScheduler = new CompactionScheduler(
                compactor,
                config,
                runtimeStats,
                versionSet::manifest,
                result -> {}
            );
            StorageEngine engine = new StorageEngine(
                config,
                sstableStore,
                versionSet,
                compactionScheduler,
                initialState.manifest().appliedThroughRevision(),
                checkpointInstaller,
                runtimeStats
            );
            engine.installCompactionResultHandler();
            return engine;
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

        memtableFlusher.throwIfFailed();
        rotationLock.lock();
        try {
            validateBatch(entries);
            for (StoredEntry entry : entries) {
                appendValidatedEntry(InternalEntry.from(entry));
            }
            long maxRevision = entries.stream()
                .mapToLong(StoredEntry::revision)
                .max()
                .orElse(0);
            appliedThroughRevision.accumulateAndGet(maxRevision, Math::max);
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
        try (ReadView view = openReadView()) {
            return view.get(key);
        }
    }

    public Scan scan(KeyRange range) {
        Objects.requireNonNull(range, "range must not be null");
        return new StorageScan(scan(toScanBounds(range)));
    }

    CloseableIterator<StoredEntry.Value> scan(ScanBounds bounds) {
        return openReadView().scan(bounds);
    }

    public StorageCheckpoint checkpoint() {
        return new StorageCheckpoint(Bytes.copyOf(checkpointBytes()));
    }

    byte[] checkpointBytes() {
        return checkpointer.checkpoint();
    }

    public void restoreInPlace(StorageCheckpoint checkpoint) {
        replaceWithCheckpoint(Objects.requireNonNull(checkpoint, "checkpoint must not be null").bytes().toByteArray());
    }

    void replaceWithCheckpoint(byte[] data) {
        checkpointer.restore(data);
    }

    public StorageMetadata metadata() {
        return new StorageMetadata(new Revision(appliedThroughRevision.get()));
    }

    public LsmStats stats() {
        refreshMemtableStats();
        return runtimeStats.snapshot();
    }

    SSTableManifest manifest() {
        return versionSet.manifest();
    }

    void awaitCompactionIdle(Duration timeout) {
        compactionScheduler.awaitIdle(timeout);
    }

    void flush() {
        memtableFlusher.throwIfFailed();
        rotationLock.lock();
        try {
            MutableMemtable current = memtables.active();
            if (current.entryCount() == 0) {
                memtableFlusher.awaitIdle();
                return;
            }
            rotateMemtable(current);
        } finally {
            rotationLock.unlock();
        }

        memtableFlusher.awaitIdle();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        flush();
        memtableFlusher.close();
        compactionScheduler.close();
        StoreVersion finalVersion = versionSet.close();
        if (!finalVersion.awaitDrain(Duration.ofSeconds(30))) {
            finalVersion.forceDrain();
        }
    }

    private void appendValidatedEntry(InternalEntry entry) {
        while (true) {
            MutableMemtable memtable = memtables.active();
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
        try (ReadView view = openReadView()) {
            Set<Slice> keys = new java.util.HashSet<>(entries.size());
            for (StoredEntry entry : entries) {
                if (!keys.add(entry.key())) {
                    throw new IllegalArgumentException("Write batch contains duplicate key: " + entry.key());
                }
                validateStoredEntry(entry, view);
            }
        }
    }

    private void tryRotateMemtable(MutableMemtable fullMemtable) {
        if (!rotationLock.tryLock()) {
            return;
        }
        try {
            MutableMemtable current = memtables.active();
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
        memtableFlusher.reserveSlot();
        ImmutableMemtable frozen = null;
        boolean scheduled = false;
        try {
            frozen = memtables.rotate(current);
            if (frozen == null) {
                return;
            }

            memtableFlusher.schedule(frozen);
            scheduled = true;
        } finally {
            if (!scheduled && frozen == null) {
                memtableFlusher.releaseReservedSlot();
            }
            refreshMemtableStats();
        }
    }

    private void resetMemtables() {
        memtables.reset();
        refreshMemtableStats();
    }

    private ReadView openReadView() {
        long snapshotRevision = appliedThroughRevision.get();
        MemtableView memtableView = memtables.captureView();
        VersionLease tables = versionSet.acquire(snapshotRevision);
        try {
            return new ReadView(memtableView, tables, snapshotRevision);
        } catch (RuntimeException e) {
            tables.close();
            throw e;
        }
    }

    private void installCompactionResultHandler() {
        compactionScheduler.setResultHandler(this::handleCompactionResult);
    }

    private void handleCompactionResult(CompactionResult result) {
        switch (result) {
            case CompactionResult.Success(var task, var outputs) -> applyCompaction(task.inputs(), outputs);
            case CompactionResult.Failure(var task, var cause) ->
                org.slf4j.LoggerFactory.getLogger(StorageEngine.class)
                    .atError()
                    .addKeyValue("targetLevel", task.targetLevel())
                    .setCause(cause)
                    .log("Compaction failed");
        }
    }

    private void applyCompaction(List<SSTableMetadata> removed, List<SSTableMetadata> added) {
        List<SSTableReader> addedReaders = sstableStore.openReaders(added);
        try {
            List<Runnable> cleanupActions = removed.isEmpty()
                ? List.of()
                : List.of(() -> sstableStore.deleteTables(removed));
            versionSet.applyCompaction(
                removed,
                added,
                addedReaders,
                appliedThroughRevision.get(),
                cleanupActions
            );
        } catch (RuntimeException e) {
            sstableStore.closeReaders(addedReaders);
            sstableStore.deleteTables(added);
            throw e;
        }
    }

    private void validateStoredEntry(StoredEntry entry, ReadView view) {
        Optional<StoredEntry> existing = view.lookupLatestEntry(entry.key());
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
        runtimeStats.updateMemtables(memtables.activeSizeInBytes(), memtables.immutableCount());
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
