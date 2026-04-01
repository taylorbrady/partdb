package io.partdb.storage;

import java.util.Objects;

public record StorageOptions(
    long writeBufferMaxBytes,
    SstableOptions sstableOptions,
    CacheOptions cacheOptions,
    CompactionOptions compactionOptions
) {

    public StorageOptions {
        if (writeBufferMaxBytes <= 0) {
            throw new IllegalArgumentException("writeBufferMaxBytes must be positive");
        }
        sstableOptions = Objects.requireNonNull(sstableOptions, "sstableOptions must not be null");
        cacheOptions = Objects.requireNonNull(cacheOptions, "cacheOptions must not be null");
        compactionOptions = Objects.requireNonNull(compactionOptions, "compactionOptions must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static StorageOptions defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    LsmConfig toLsmConfig() {
        return new LsmConfig(
            writeBufferMaxBytes,
            sstableOptions.blockSizeBytes(),
            sstableOptions.blockRestartInterval(),
            sstableOptions.bloomFilterFalsePositiveRate(),
            sstableOptions.compression().toBlockCodec(),
            cacheOptions.blockCacheMaxBytes(),
            compactionOptions.targetTableSizeBytes(),
            compactionOptions.maxConcurrentCompactions(),
            compactionOptions.l0CompactionTrigger(),
            compactionOptions.maxBytesForLevelBase(),
            compactionOptions.levelMultiplier(),
            compactionOptions.maxLevels()
        );
    }

    public static final class Builder {
        private long writeBufferMaxBytes = LsmConfig.DEFAULT_MEMTABLE_MAX_SIZE_BYTES;
        private SstableOptions sstableOptions = SstableOptions.defaults();
        private CacheOptions cacheOptions = CacheOptions.defaults();
        private CompactionOptions compactionOptions = CompactionOptions.defaults();

        private Builder() {
        }

        private Builder(StorageOptions options) {
            this.writeBufferMaxBytes = options.writeBufferMaxBytes;
            this.sstableOptions = options.sstableOptions;
            this.cacheOptions = options.cacheOptions;
            this.compactionOptions = options.compactionOptions;
        }

        public Builder writeBufferMaxBytes(long writeBufferMaxBytes) {
            this.writeBufferMaxBytes = writeBufferMaxBytes;
            return this;
        }

        public Builder sstableOptions(SstableOptions sstableOptions) {
            this.sstableOptions = Objects.requireNonNull(sstableOptions, "sstableOptions must not be null");
            return this;
        }

        public Builder cacheOptions(CacheOptions cacheOptions) {
            this.cacheOptions = Objects.requireNonNull(cacheOptions, "cacheOptions must not be null");
            return this;
        }

        public Builder compactionOptions(CompactionOptions compactionOptions) {
            this.compactionOptions = Objects.requireNonNull(compactionOptions, "compactionOptions must not be null");
            return this;
        }

        public StorageOptions build() {
            return new StorageOptions(writeBufferMaxBytes, sstableOptions, cacheOptions, compactionOptions);
        }
    }
}
