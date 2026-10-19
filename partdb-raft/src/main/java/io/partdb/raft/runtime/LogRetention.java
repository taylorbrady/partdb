package io.partdb.raft.runtime;

public record LogRetention(long entries, long bytes) {
    public static final LogRetention DEFAULT = new LogRetention(10_000, 64L << 20);

    public LogRetention {
        if (entries <= 0) {
            throw new IllegalArgumentException("entries must be positive");
        }
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be positive");
        }
    }

    public static LogRetention ofEntries(long entries) {
        return new LogRetention(entries, DEFAULT.bytes());
    }
}
