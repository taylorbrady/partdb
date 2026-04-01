package io.partdb.storage;

public record CacheOptions(long blockCacheMaxBytes) {

    public CacheOptions {
        if (blockCacheMaxBytes < 0) {
            throw new IllegalArgumentException("blockCacheMaxBytes must be non-negative");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CacheOptions defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private long blockCacheMaxBytes = LsmConfig.DEFAULT_BLOCK_CACHE_MAX_BYTES;

        private Builder() {
        }

        private Builder(CacheOptions options) {
            this.blockCacheMaxBytes = options.blockCacheMaxBytes;
        }

        public Builder blockCacheMaxBytes(long blockCacheMaxBytes) {
            this.blockCacheMaxBytes = blockCacheMaxBytes;
            return this;
        }

        public CacheOptions build() {
            return new CacheOptions(blockCacheMaxBytes);
        }
    }
}
