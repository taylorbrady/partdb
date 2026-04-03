package io.partdb.node.metrics;

import java.time.Duration;
import java.util.Objects;

public record StorageMetrics(
    long activeMemtableBytes,
    int immutableMemtableCount,
    int sstableCount,
    long totalSstableBytes,
    int activeCompactions,
    long completedCompactions,
    long failedCompactions,
    Duration lastCompactionDuration,
    long checkpointCount,
    long restoreCount,
    Duration lastCheckpointDuration,
    Duration lastRestoreDuration
) {
    public StorageMetrics {
        Objects.requireNonNull(lastCompactionDuration, "lastCompactionDuration must not be null");
        Objects.requireNonNull(lastCheckpointDuration, "lastCheckpointDuration must not be null");
        Objects.requireNonNull(lastRestoreDuration, "lastRestoreDuration must not be null");
    }
}
