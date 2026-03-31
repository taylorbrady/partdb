package io.partdb.storage;

public record StorageEngineStats(
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
