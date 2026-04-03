package io.partdb.node.config;

import io.partdb.storage.CacheOptions;
import io.partdb.storage.CompactionOptions;
import io.partdb.storage.SstableOptions;
import io.partdb.storage.StorageOptions;

import java.util.Objects;

public record StorageConfig(
    long writeBufferMaxBytes,
    SstableConfig sstable,
    CacheConfig cache,
    CompactionConfig compaction
) {
    public StorageConfig {
        if (writeBufferMaxBytes <= 0) {
            throw new IllegalArgumentException("writeBufferMaxBytes must be positive");
        }
        sstable = Objects.requireNonNull(sstable, "sstable must not be null");
        cache = Objects.requireNonNull(cache, "cache must not be null");
        compaction = Objects.requireNonNull(compaction, "compaction must not be null");
    }

    public static StorageConfig defaults() {
        return fromStorageOptions(StorageOptions.defaults());
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public StorageOptions toStorageOptions() {
        return StorageOptions.builder()
            .writeBufferMaxBytes(writeBufferMaxBytes)
            .sstableOptions(new SstableOptions(
                sstable.blockSizeBytes(),
                sstable.blockRestartInterval(),
                sstable.bloomFilterFalsePositiveRate(),
                switch (sstable.compression()) {
                    case NONE -> SstableOptions.Compression.NONE;
                    case DEFLATE -> SstableOptions.Compression.DEFLATE;
                }
            ))
            .cacheOptions(new CacheOptions(cache.blockCacheMaxBytes()))
            .compactionOptions(new CompactionOptions(
                compaction.targetTableSizeBytes(),
                compaction.maxConcurrentCompactions(),
                compaction.levelZeroCompactionTrigger(),
                compaction.maxBytesForLevelBase(),
                compaction.levelMultiplier(),
                compaction.maxLevels()
            ))
            .build();
    }

    public static StorageConfig fromStorageOptions(StorageOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        return new StorageConfig(
            options.writeBufferMaxBytes(),
            new SstableConfig(
                options.sstableOptions().blockSizeBytes(),
                options.sstableOptions().blockRestartInterval(),
                options.sstableOptions().bloomFilterFalsePositiveRate(),
                switch (options.sstableOptions().compression()) {
                    case NONE -> Compression.NONE;
                    case DEFLATE -> Compression.DEFLATE;
                }
            ),
            new CacheConfig(options.cacheOptions().blockCacheMaxBytes()),
            new CompactionConfig(
                options.compactionOptions().targetTableSizeBytes(),
                options.compactionOptions().maxConcurrentCompactions(),
                options.compactionOptions().l0CompactionTrigger(),
                options.compactionOptions().maxBytesForLevelBase(),
                options.compactionOptions().levelMultiplier(),
                options.compactionOptions().maxLevels()
            )
        );
    }

    public enum Compression {
        NONE,
        DEFLATE
    }

    public record SstableConfig(
        int blockSizeBytes,
        int blockRestartInterval,
        double bloomFilterFalsePositiveRate,
        Compression compression
    ) {
        public SstableConfig {
            if (blockSizeBytes <= 0) {
                throw new IllegalArgumentException("blockSizeBytes must be positive");
            }
            if (blockRestartInterval <= 0) {
                throw new IllegalArgumentException("blockRestartInterval must be positive");
            }
            if (bloomFilterFalsePositiveRate <= 0 || bloomFilterFalsePositiveRate >= 1) {
                throw new IllegalArgumentException("bloomFilterFalsePositiveRate must be between 0 and 1");
            }
            compression = Objects.requireNonNull(compression, "compression must not be null");
        }

        public static SstableConfig defaults() {
            return StorageConfig.defaults().sstable();
        }
    }

    public record CacheConfig(long blockCacheMaxBytes) {
        public CacheConfig {
            if (blockCacheMaxBytes < 0) {
                throw new IllegalArgumentException("blockCacheMaxBytes must not be negative");
            }
        }

        public static CacheConfig defaults() {
            return StorageConfig.defaults().cache();
        }
    }

    public record CompactionConfig(
        long targetTableSizeBytes,
        int maxConcurrentCompactions,
        int levelZeroCompactionTrigger,
        long maxBytesForLevelBase,
        int levelMultiplier,
        int maxLevels
    ) {
        public CompactionConfig {
            if (targetTableSizeBytes <= 0) {
                throw new IllegalArgumentException("targetTableSizeBytes must be positive");
            }
            if (maxConcurrentCompactions <= 0) {
                throw new IllegalArgumentException("maxConcurrentCompactions must be positive");
            }
            if (levelZeroCompactionTrigger <= 0) {
                throw new IllegalArgumentException("levelZeroCompactionTrigger must be positive");
            }
            if (maxBytesForLevelBase <= 0) {
                throw new IllegalArgumentException("maxBytesForLevelBase must be positive");
            }
            if (levelMultiplier <= 1) {
                throw new IllegalArgumentException("levelMultiplier must be greater than 1");
            }
            if (maxLevels <= 0) {
                throw new IllegalArgumentException("maxLevels must be positive");
            }
        }

        public static CompactionConfig defaults() {
            return StorageConfig.defaults().compaction();
        }
    }

    public static final class Builder {
        private long writeBufferMaxBytes = StorageOptions.defaults().writeBufferMaxBytes();
        private SstableConfig sstable = StorageConfig.defaults().sstable();
        private CacheConfig cache = StorageConfig.defaults().cache();
        private CompactionConfig compaction = StorageConfig.defaults().compaction();

        private Builder() {
        }

        private Builder(StorageConfig config) {
            this.writeBufferMaxBytes = config.writeBufferMaxBytes;
            this.sstable = config.sstable;
            this.cache = config.cache;
            this.compaction = config.compaction;
        }

        public Builder writeBufferMaxBytes(long writeBufferMaxBytes) {
            this.writeBufferMaxBytes = writeBufferMaxBytes;
            return this;
        }

        public Builder sstable(SstableConfig sstable) {
            this.sstable = Objects.requireNonNull(sstable, "sstable must not be null");
            return this;
        }

        public Builder cache(CacheConfig cache) {
            this.cache = Objects.requireNonNull(cache, "cache must not be null");
            return this;
        }

        public Builder compaction(CompactionConfig compaction) {
            this.compaction = Objects.requireNonNull(compaction, "compaction must not be null");
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(writeBufferMaxBytes, sstable, cache, compaction);
        }
    }
}
