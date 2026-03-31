package io.partdb.storage;

public record LsmStats(
    long activeMemtableBytes,
    int immutableMemtableCount,
    int sstableCount,
    long totalSstableBytes,
    int activeCompactions,
    long completedCompactions,
    long failedCompactions,
    long lastCompactionDurationMillis,
    long checkpointCount,
    long restoreCount,
    long lastCheckpointDurationMillis,
    long lastRestoreDurationMillis
) {
}
