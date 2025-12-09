package io.partdb.storage.compaction;

public record CompactionConfig(long targetSSTableSize) {

    public static final long DEFAULT_TARGET_SIZE = 64 * 1024 * 1024;

    public CompactionConfig {
        if (targetSSTableSize <= 0) {
            throw new IllegalArgumentException("targetSSTableSize must be positive");
        }
    }

    public static CompactionConfig defaults() {
        return new CompactionConfig(DEFAULT_TARGET_SIZE);
    }
}
