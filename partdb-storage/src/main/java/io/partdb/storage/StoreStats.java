package io.partdb.storage;

import java.time.Duration;
import java.util.Objects;

public record StoreStats(
    long activeMemtableBytes,
    int immutableMemtableCount,
    int tableCount,
    long tableBytes,
    int activeCompactions,
    long completedCompactions,
    long failedCompactions,
    Duration lastCompactionDuration,
    long checkpointCount,
    long restoreCount,
    Duration lastCheckpointDuration,
    Duration lastRestoreDuration
) {

    public StoreStats {
        Objects.requireNonNull(lastCompactionDuration, "lastCompactionDuration must not be null");
        Objects.requireNonNull(lastCheckpointDuration, "lastCheckpointDuration must not be null");
        Objects.requireNonNull(lastRestoreDuration, "lastRestoreDuration must not be null");
    }
}
