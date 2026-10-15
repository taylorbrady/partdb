package io.partdb.server;

public interface StorageMXBean {
    long getActiveMemtableBytes();

    int getImmutableMemtableCount();

    int getSstableCount();

    long getTotalSstableBytes();

    int getActiveCompactions();

    long getCompletedCompactions();

    long getFailedCompactions();

    long getLastCompactionDurationMillis();

    long getCheckpointCount();

    long getRestoreCount();

    long getLastCheckpointDurationMillis();

    long getLastRestoreDurationMillis();
}
