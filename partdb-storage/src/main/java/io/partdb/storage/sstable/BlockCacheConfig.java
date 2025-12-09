package io.partdb.storage.sstable;

public record BlockCacheConfig(long maxSizeInBytes) {

    public static final long DEFAULT_MAX_SIZE = 64 * 1024 * 1024;

    public BlockCacheConfig {
        if (maxSizeInBytes < 1024 * 1024) {
            throw new IllegalArgumentException("maxSizeInBytes must be at least 1MB");
        }
    }

    public static BlockCacheConfig defaults() {
        return new BlockCacheConfig(DEFAULT_MAX_SIZE);
    }
}
