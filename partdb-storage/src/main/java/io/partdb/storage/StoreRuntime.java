package io.partdb.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private final TableCatalog tableCatalog;
    private final FlushCoordinator flushCoordinator;
    private final StorageRuntimeStats stats;
    private final ReadCoordinator readCoordinator;
    private final CheckpointManager checkpointManager;

    private StoreRuntime(LsmConfig config, TableCatalog tableCatalog, StorageRuntimeStats stats) {
        this.config = config;
        this.tableCatalog = tableCatalog;
        this.stats = stats;
        this.activeMemtable = new AtomicReference<>(new MutableMemtable());
        this.rotationLock = new ReentrantLock();
        this.closed = new AtomicBoolean(false);
        this.flushCoordinator = new FlushCoordinator(tableCatalog, this::refreshMemtableStats);
        this.readCoordinator = new ReadCoordinator(activeMemtable::get, flushCoordinator::immutableMemtables, tableCatalog);
        this.checkpointManager = new CheckpointManager(
            tableCatalog,
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
            TableCatalog tableCatalog = TableCatalog.open(dataDirectory, config, stats);
            return new StoreRuntime(config, tableCatalog, stats);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open store", e);
        }
    }

    void put(Slice key, Slice value, long revision) {
        applyMutation(new Mutation.Put(key, value, revision));
    }

    void delete(Slice key, long revision) {
        applyMutation(new Mutation.Tombstone(key, revision));
    }

    Optional<EngineEntry> get(Slice key) {
        return readCoordinator.get(key);
    }

    EngineEntryCursor scan(ScanBounds bounds) {
        return readCoordinator.scan(bounds);
    }

    byte[] checkpoint() {
        return checkpointManager.checkpoint();
    }

    void replaceWithCheckpoint(byte[] data) {
        checkpointManager.restore(data);
    }

    SSTableManifest manifest() {
        return tableCatalog.manifest();
    }

    LsmStats statsSnapshot() {
        refreshMemtableStats();
        return stats.snapshot();
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
        tableCatalog.close();
    }

    static Optional<Mutation> lookupMutation(
        Slice key,
        MutableMemtable activeMemtable,
        List<ImmutableMemtable> immutableMemtables
    ) {
        return ReadCoordinator.lookupMutation(key, activeMemtable, immutableMemtables);
    }

    private void applyMutation(Mutation mutation) {
        ValidationResult validation = validateMutation(mutation);
        if (validation == ValidationResult.DUPLICATE) {
            return;
        }

        while (true) {
            MutableMemtable memtable = activeMemtable.get();
            MutableMemtable.WriteResult writeResult = memtable.put(mutation);
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

    private ValidationResult validateMutation(Mutation mutation) {
        Optional<Mutation> existing = readCoordinator.lookupVisibleMutation(mutation.key());
        if (existing.isEmpty()) {
            return ValidationResult.APPLY;
        }

        Mutation current = existing.get();
        if (mutation.revision() > current.revision()) {
            return ValidationResult.APPLY;
        }

        if (mutation.revision() == current.revision() && mutation.equals(current)) {
            return ValidationResult.DUPLICATE;
        }

        if (mutation.revision() < current.revision()) {
            throw new StorageException.InvalidRevision(
                "Revision %d for key %s is older than current revision %d"
                    .formatted(mutation.revision(), mutation.key(), current.revision())
            );
        }

        throw new StorageException.InvalidRevision(
            "Conflicting mutation for key %s at revision %d"
                .formatted(mutation.key(), mutation.revision())
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
