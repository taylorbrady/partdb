package io.partdb.raft.runtime;

public record SnapshotPolicy(long logEntries, long logBytes) {
    public static final SnapshotPolicy DEFAULT = new SnapshotPolicy(10_000, 64L << 20);

    public SnapshotPolicy {
        if (logEntries <= 0) {
            throw new IllegalArgumentException("logEntries must be positive");
        }
        if (logBytes <= 0) {
            throw new IllegalArgumentException("logBytes must be positive");
        }
    }

    public static SnapshotPolicy everyEntries(long entries) {
        return new SnapshotPolicy(entries, DEFAULT.logBytes());
    }

    boolean due(long entries, long bytes) {
        return entries >= logEntries || bytes >= logBytes;
    }
}
