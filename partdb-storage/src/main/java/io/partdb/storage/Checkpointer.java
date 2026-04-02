package io.partdb.storage;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

final class Checkpointer {

    private final VersionSet versionSet;
    private final CheckpointInstaller checkpointInstaller;
    private final CompactionScheduler compactionScheduler;
    private final MemtableFlusher memtableFlusher;
    private final ReentrantLock rotationLock;
    private final Runnable resetMemtables;
    private final Runnable flushStore;
    private final LongSupplier appliedThroughRevision;
    private final LongConsumer restoreAppliedThroughRevision;
    private final StorageRuntimeStats stats;

    Checkpointer(
        VersionSet versionSet,
        CheckpointInstaller checkpointInstaller,
        CompactionScheduler compactionScheduler,
        MemtableFlusher memtableFlusher,
        ReentrantLock rotationLock,
        Runnable resetMemtables,
        Runnable flushStore,
        LongSupplier appliedThroughRevision,
        LongConsumer restoreAppliedThroughRevision,
        StorageRuntimeStats stats
    ) {
        this.versionSet = Objects.requireNonNull(versionSet, "versionSet");
        this.checkpointInstaller = Objects.requireNonNull(checkpointInstaller, "checkpointInstaller");
        this.compactionScheduler = Objects.requireNonNull(compactionScheduler, "compactionScheduler");
        this.memtableFlusher = Objects.requireNonNull(memtableFlusher, "memtableFlusher");
        this.rotationLock = Objects.requireNonNull(rotationLock, "rotationLock");
        this.resetMemtables = Objects.requireNonNull(resetMemtables, "resetMemtables");
        this.flushStore = Objects.requireNonNull(flushStore, "flushStore");
        this.appliedThroughRevision = Objects.requireNonNull(appliedThroughRevision, "appliedThroughRevision");
        this.restoreAppliedThroughRevision = Objects.requireNonNull(restoreAppliedThroughRevision, "restoreAppliedThroughRevision");
        this.stats = Objects.requireNonNull(stats, "stats");
    }

    byte[] checkpoint() {
        StorageCheckpointEvent event = new StorageCheckpointEvent();
        event.phase = "checkpoint";
        event.begin();

        long startNanos = System.nanoTime();
        try {
            flushStore.run();
            byte[] checkpoint;
            try (VersionLease snapshot = versionSet.acquire(appliedThroughRevision.getAsLong())) {
                checkpoint = VersionCheckpoint.capture(snapshot).toBytes();
            }
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
        VersionCheckpoint checkpoint = VersionCheckpoint.fromBytes(data);

        StorageCheckpointEvent event = new StorageCheckpointEvent();
        event.phase = "restore";
        event.bytes = data.length;
        event.begin();

        long startNanos = System.nanoTime();
        rotationLock.lock();
        try {
            memtableFlusher.awaitIdle();
            restore(checkpoint);
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

    private void restore(VersionCheckpoint checkpoint) {
        boolean restoredStateInstalled = false;
        boolean backedUpCurrentState = false;

        try {
            try {
                checkpointInstaller.stageAndValidate(checkpoint);
            } catch (java.io.IOException e) {
                throw new StorageException.IO("Failed to stage checkpoint files", e);
            }

            try (CompactionScheduler.Pause ignored = compactionScheduler.pauseAndAwaitIdle(java.time.Duration.ofSeconds(30))) {
                SSTableManifest previousManifest = versionSet.manifest();
                StoreVersion previousVersion = versionSet.retireCurrent();

                if (!previousVersion.awaitDrain(java.time.Duration.ofSeconds(30))) {
                    LoadedStoreVersion liveState = checkpointInstaller.restoreLive(previousManifest);
                    versionSet.replaceCurrent(liveState);
                    restoreAppliedThroughRevision.accept(liveState.manifest().appliedThroughRevision());
                    throw new StorageException.Timeout(
                        "Timed out waiting for store version readers to drain during restore",
                        null
                    );
                }

                try {
                    checkpointInstaller.backupCurrentState(previousManifest);
                    backedUpCurrentState = true;
                } catch (java.io.IOException e) {
                    throw new StorageException.IO("Failed to restore checkpoint", e);
                }

                try {
                    LoadedStoreVersion restored = checkpointInstaller.activate(checkpoint);
                    versionSet.replaceCurrent(restored);
                    restoreAppliedThroughRevision.accept(restored.manifest().appliedThroughRevision());
                    restoredStateInstalled = true;
                } catch (java.io.IOException e) {
                    rollbackRestore(previousManifest, backedUpCurrentState);
                    throw new StorageException.IO("Failed to restore checkpoint", e);
                } catch (RuntimeException e) {
                    rollbackRestore(previousManifest, backedUpCurrentState);
                    throw e;
                }

                if (restoredStateInstalled) {
                    compactionScheduler.requestCompaction();
                    try {
                        checkpointInstaller.cleanupBackups(previousManifest);
                    } catch (java.io.IOException e) {
                        org.slf4j.LoggerFactory.getLogger(Checkpointer.class)
                            .atWarn()
                            .setCause(e)
                            .log("Failed to clean restore backup files");
                    }
                }
            }
        } finally {
            checkpointInstaller.cleanupStaged(checkpoint);
        }
    }

    private void rollbackRestore(SSTableManifest previousManifest, boolean backedUpCurrentState) {
        try {
            LoadedStoreVersion restored = checkpointInstaller.rollback(previousManifest, backedUpCurrentState);
            versionSet.replaceCurrent(restored);
            restoreAppliedThroughRevision.accept(restored.manifest().appliedThroughRevision());
        } catch (java.io.IOException e) {
            throw new StorageException.IO("Failed to restore checkpoint", e);
        }
    }
}
