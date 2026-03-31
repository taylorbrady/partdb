package io.partdb.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

final class StoreRuntime implements AutoCloseable {

    private final LsmConfig config;
    private final AtomicReference<MutableMemtable> activeMemtable;
    private final ReentrantLock rotationLock;
    private final AtomicBoolean closed;
    private final SSTableCatalog sstableCatalog;
    private final FlushCoordinator flushCoordinator;
    private final StorageRuntimeStats stats;
    private final ReadCoordinator readCoordinator;
    private final CheckpointService checkpointService;

    private StoreRuntime(LsmConfig config, SSTableCatalog sstableCatalog, StorageRuntimeStats stats) {
        this.config = config;
        this.sstableCatalog = sstableCatalog;
        this.stats = stats;
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
            stats
        );
        refreshMemtableStats();
    }

    static StoreRuntime open(Path dataDirectory, LsmConfig config) {
        try {
            Files.createDirectories(dataDirectory);
            StorageRuntimeStats stats = new StorageRuntimeStats();
            SSTableCatalog sstableCatalog = SSTableCatalog.open(dataDirectory, config, stats);
            return new StoreRuntime(config, sstableCatalog, stats);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open store", e);
        }
    }

    void put(Slice key, Slice value, long revision) {
        applyStoredEntry(new StoredEntry.Value(key, value, revision));
    }

    void delete(Slice key, long revision) {
        applyStoredEntry(new StoredEntry.Tombstone(key, revision));
    }

    Optional<StoredEntry.Value> get(Slice key) {
        return readCoordinator.get(key);
    }

    StoredValueCursor scan(ScanBounds bounds) {
        return readCoordinator.scan(bounds);
    }

    byte[] checkpoint() {
        return checkpointService.checkpoint();
    }

    void replaceWithCheckpoint(byte[] data) {
        checkpointService.restore(data);
    }

    SSTableManifest manifest() {
        return sstableCatalog.manifest();
    }

    LsmStats statsSnapshot() {
        refreshMemtableStats();
        return stats.snapshot();
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

    static Optional<StoredEntry> lookupStoredEntry(
        Slice key,
        MutableMemtable activeMemtable,
        List<ImmutableMemtable> immutableMemtables
    ) {
        return ReadCoordinator.lookupStoredEntry(key, activeMemtable, immutableMemtables);
    }

    private void applyStoredEntry(StoredEntry entry) {
        ValidationResult validation = validateStoredEntry(entry);
        if (validation == ValidationResult.DUPLICATE) {
            return;
        }

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

            if (memtable.sizeInBytes() >= config.memtableMaxSizeBytes()) {
                tryRotateMemtable(memtable);
            }
            return;
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
            if (current.sizeInBytes() < config.memtableMaxSizeBytes()) {
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

    private ValidationResult validateStoredEntry(StoredEntry entry) {
        Optional<StoredEntry> existing = readCoordinator.lookupLatestEntry(entry.key());
        if (existing.isEmpty()) {
            return ValidationResult.APPLY;
        }

        StoredEntry current = existing.get();
        if (entry.revision() > current.revision()) {
            return ValidationResult.APPLY;
        }

        if (entry.revision() == current.revision() && entry.equals(current)) {
            return ValidationResult.DUPLICATE;
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

    private enum ValidationResult {
        APPLY,
        DUPLICATE
    }

    private void refreshMemtableStats() {
        stats.updateMemtables(activeMemtable.get().sizeInBytes(), flushCoordinator.immutableMemtables().size());
    }
}
