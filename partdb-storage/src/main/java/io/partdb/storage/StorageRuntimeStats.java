package io.partdb.storage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class StorageRuntimeStats {
    private final AtomicLong activeMemtableBytes = new AtomicLong();
    private final AtomicInteger immutableMemtableCount = new AtomicInteger();
    private final AtomicInteger sstableCount = new AtomicInteger();
    private final AtomicLong totalSstableBytes = new AtomicLong();
    private final AtomicInteger activeCompactions = new AtomicInteger();
    private final AtomicLong completedCompactions = new AtomicLong();
    private final AtomicLong failedCompactions = new AtomicLong();
    private final AtomicLong lastCompactionDurationMillis = new AtomicLong();
    private final AtomicLong checkpointCount = new AtomicLong();
    private final AtomicLong restoreCount = new AtomicLong();
    private final AtomicLong lastCheckpointDurationMillis = new AtomicLong();
    private final AtomicLong lastRestoreDurationMillis = new AtomicLong();

    void updateMemtables(long activeMemtableBytes, int immutableMemtableCount) {
        this.activeMemtableBytes.set(activeMemtableBytes);
        this.immutableMemtableCount.set(immutableMemtableCount);
    }

    void updateSstables(SSTableManifest manifest) {
        sstableCount.set(manifest.sstables().size());
        totalSstableBytes.set(
            manifest.sstables().stream()
                .mapToLong(SSTableMetadata::fileSizeBytes)
                .sum()
        );
    }

    void compactionStarted() {
        activeCompactions.incrementAndGet();
    }

    void compactionFinished(boolean success, long durationMillis) {
        activeCompactions.updateAndGet(current -> Math.max(0, current - 1));
        if (success) {
            completedCompactions.incrementAndGet();
        } else {
            failedCompactions.incrementAndGet();
        }
        lastCompactionDurationMillis.set(durationMillis);
    }

    void checkpointFinished(long durationMillis) {
        checkpointCount.incrementAndGet();
        lastCheckpointDurationMillis.set(durationMillis);
    }

    void restoreFinished(long durationMillis) {
        restoreCount.incrementAndGet();
        lastRestoreDurationMillis.set(durationMillis);
    }

    StorageEngineStats snapshot() {
        return new StorageEngineStats(
            activeMemtableBytes.get(),
            immutableMemtableCount.get(),
            sstableCount.get(),
            totalSstableBytes.get(),
            activeCompactions.get(),
            completedCompactions.get(),
            failedCompactions.get(),
            lastCompactionDurationMillis.get(),
            checkpointCount.get(),
            restoreCount.get(),
            lastCheckpointDurationMillis.get(),
            lastRestoreDurationMillis.get()
        );
    }
}
