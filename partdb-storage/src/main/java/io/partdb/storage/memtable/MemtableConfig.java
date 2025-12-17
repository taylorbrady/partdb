package io.partdb.storage.memtable;

public record MemtableConfig(long maxSizeInBytes) {

    public static final long DEFAULT_MAX_SIZE = 64 * 1024 * 1024;

    public MemtableConfig {
        if (maxSizeInBytes <= 0) {
            throw new IllegalArgumentException("maxSizeInBytes must be positive");
        }
    }

    public static MemtableConfig defaults() {
        return new MemtableConfig(DEFAULT_MAX_SIZE);
    }

    public MemtableConfig withMaxSizeInBytes(long maxSizeInBytes) {
        return new MemtableConfig(maxSizeInBytes);
    }
}
