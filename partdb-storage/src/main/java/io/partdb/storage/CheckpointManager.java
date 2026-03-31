package io.partdb.storage;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

final class CheckpointManager {

    private final TableCatalog tableCatalog;
    private final FlushCoordinator flushCoordinator;
    private final ReentrantLock rotationLock;
    private final Runnable resetMemtables;
    private final Runnable flushStore;
    private final StorageRuntimeStats stats;

    CheckpointManager(
        TableCatalog tableCatalog,
        FlushCoordinator flushCoordinator,
        ReentrantLock rotationLock,
        Runnable resetMemtables,
        Runnable flushStore,
        StorageRuntimeStats stats
    ) {
        this.tableCatalog = Objects.requireNonNull(tableCatalog, "tableCatalog");
        this.flushCoordinator = Objects.requireNonNull(flushCoordinator, "flushCoordinator");
        this.rotationLock = Objects.requireNonNull(rotationLock, "rotationLock");
        this.resetMemtables = Objects.requireNonNull(resetMemtables, "resetMemtables");
        this.flushStore = Objects.requireNonNull(flushStore, "flushStore");
        this.stats = Objects.requireNonNull(stats, "stats");
    }

    byte[] checkpoint() {
        StorageCheckpointEvent event = new StorageCheckpointEvent();
        event.phase = "checkpoint";
        event.begin();

        long startNanos = System.nanoTime();
        try {
            flushStore.run();
            byte[] checkpoint = tableCatalog.captureCheckpoint().toBytes();
            stats.checkpointFinished((System.nanoTime() - startNanos) / 1_000_000);
            event.bytes = checkpoint.length;
            event.success = true;
            return checkpoint;
        } catch (RuntimeException e) {
            event.success = false;
            event.error = e.getClass().getSimpleName();
            throw e;
        } finally {
            event.commit();
        }
    }

    void restore(byte[] data) {
        CatalogCheckpoint checkpoint = CatalogCheckpoint.fromBytes(data);

        StorageCheckpointEvent event = new StorageCheckpointEvent();
        event.phase = "restore";
        event.bytes = data.length;
        event.begin();

        long startNanos = System.nanoTime();
        rotationLock.lock();
        try {
            flushCoordinator.awaitIdle();
            tableCatalog.replaceWith(checkpoint);
            resetMemtables.run();
            stats.restoreFinished((System.nanoTime() - startNanos) / 1_000_000);
            event.success = true;
        } catch (RuntimeException e) {
            event.success = false;
            event.error = e.getClass().getSimpleName();
            throw e;
        } finally {
            rotationLock.unlock();
            event.commit();
        }
    }
}
