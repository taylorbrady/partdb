package io.partdb.storage;

import java.util.Objects;

record LsmConfig(
    long memtableMaxSizeBytes,
    int blockSize,
    int blockRestartInterval,
    double bloomFilterFalsePositiveRate,
    BlockCodec blockCodec,
    long blockCacheMaxBytes,
    long targetUncompressedSize,
    int maxConcurrentCompactions,
    int l0CompactionTrigger,
    long maxBytesForLevelBase,
    int levelMultiplier,
    int maxLevels
) {

    public static final long DEFAULT_MEMTABLE_MAX_SIZE_BYTES = 64 * 1024 * 1024;
    public static final int DEFAULT_BLOCK_SIZE = 32 * 1024;
    public static final int DEFAULT_BLOCK_RESTART_INTERVAL = 16;
    public static final double DEFAULT_BLOOM_FILTER_FPR = 0.01;
    public static final long DEFAULT_BLOCK_CACHE_MAX_BYTES = 64 * 1024 * 1024;
    public static final long DEFAULT_TARGET_UNCOMPRESSED_SIZE = 64 * 1024 * 1024;
    public static final int DEFAULT_MAX_CONCURRENT_COMPACTIONS = 4;
    public static final int DEFAULT_L0_COMPACTION_TRIGGER = 4;
    public static final long DEFAULT_MAX_BYTES_FOR_LEVEL_BASE = 10 * 1024 * 1024;
    public static final int DEFAULT_LEVEL_MULTIPLIER = 10;
    public static final int DEFAULT_MAX_LEVELS = 7;

    public LsmConfig {
        if (memtableMaxSizeBytes <= 0) {
            throw new IllegalArgumentException("memtableMaxSizeBytes must be positive");
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }
        if (blockRestartInterval <= 0) {
            throw new IllegalArgumentException("blockRestartInterval must be positive");
        }
        if (bloomFilterFalsePositiveRate <= 0 || bloomFilterFalsePositiveRate >= 1) {
            throw new IllegalArgumentException("bloomFilterFalsePositiveRate must be between 0 and 1");
        }
        Objects.requireNonNull(blockCodec, "blockCodec must not be null");
        if (blockCacheMaxBytes < 0) {
            throw new IllegalArgumentException("blockCacheMaxBytes must be non-negative");
        }
        if (targetUncompressedSize <= 0) {
            throw new IllegalArgumentException("targetUncompressedSize must be positive");
        }
        if (maxConcurrentCompactions <= 0) {
            throw new IllegalArgumentException("maxConcurrentCompactions must be positive");
        }
        if (l0CompactionTrigger <= 0) {
            throw new IllegalArgumentException("l0CompactionTrigger must be positive");
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

    public static Builder builder() {
        return new Builder();
    }

    public static LsmConfig defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public LsmConfig withMemtableMaxSizeBytes(long memtableMaxSizeBytes) {
        return toBuilder().memtableMaxSizeBytes(memtableMaxSizeBytes).build();
    }

    public LsmConfig withBlockSize(int blockSize) {
        return toBuilder().blockSize(blockSize).build();
    }

    public LsmConfig withBlockRestartInterval(int blockRestartInterval) {
        return toBuilder().blockRestartInterval(blockRestartInterval).build();
    }

    public LsmConfig withBloomFilterFalsePositiveRate(double bloomFilterFalsePositiveRate) {
        return toBuilder().bloomFilterFalsePositiveRate(bloomFilterFalsePositiveRate).build();
    }

    public LsmConfig withBlockCodec(BlockCodec blockCodec) {
        return toBuilder().blockCodec(blockCodec).build();
    }

    public LsmConfig withBlockCacheMaxBytes(long blockCacheMaxBytes) {
        return toBuilder().blockCacheMaxBytes(blockCacheMaxBytes).build();
    }

    public LsmConfig withTargetUncompressedSize(long targetUncompressedSize) {
        return toBuilder().targetUncompressedSize(targetUncompressedSize).build();
    }

    public LsmConfig withMaxConcurrentCompactions(int maxConcurrentCompactions) {
        return toBuilder().maxConcurrentCompactions(maxConcurrentCompactions).build();
    }

    public LsmConfig withL0CompactionTrigger(int l0CompactionTrigger) {
        return toBuilder().l0CompactionTrigger(l0CompactionTrigger).build();
    }

    public LsmConfig withMaxBytesForLevelBase(long maxBytesForLevelBase) {
        return toBuilder().maxBytesForLevelBase(maxBytesForLevelBase).build();
    }

    public LsmConfig withLevelMultiplier(int levelMultiplier) {
        return toBuilder().levelMultiplier(levelMultiplier).build();
    }

    public LsmConfig withMaxLevels(int maxLevels) {
        return toBuilder().maxLevels(maxLevels).build();
    }

    public boolean cacheEnabled() {
        return blockCacheMaxBytes > 0;
    }

    public long maxBytesForLevel(int level) {
        if (level == 0) {
            return Long.MAX_VALUE;
        }
        return maxBytesForLevelBase * (long) Math.pow(levelMultiplier, level - 1);
    }

    public static final class Builder {
        private long memtableMaxSizeBytes = DEFAULT_MEMTABLE_MAX_SIZE_BYTES;
        private int blockSize = DEFAULT_BLOCK_SIZE;
        private int blockRestartInterval = DEFAULT_BLOCK_RESTART_INTERVAL;
        private double bloomFilterFalsePositiveRate = DEFAULT_BLOOM_FILTER_FPR;
        private BlockCodec blockCodec = BlockCodec.DEFLATE;
        private long blockCacheMaxBytes = DEFAULT_BLOCK_CACHE_MAX_BYTES;
        private long targetUncompressedSize = DEFAULT_TARGET_UNCOMPRESSED_SIZE;
        private int maxConcurrentCompactions = DEFAULT_MAX_CONCURRENT_COMPACTIONS;
        private int l0CompactionTrigger = DEFAULT_L0_COMPACTION_TRIGGER;
        private long maxBytesForLevelBase = DEFAULT_MAX_BYTES_FOR_LEVEL_BASE;
        private int levelMultiplier = DEFAULT_LEVEL_MULTIPLIER;
        private int maxLevels = DEFAULT_MAX_LEVELS;

        private Builder() {
        }

        private Builder(LsmConfig config) {
            this.memtableMaxSizeBytes = config.memtableMaxSizeBytes;
            this.blockSize = config.blockSize;
            this.blockRestartInterval = config.blockRestartInterval;
            this.bloomFilterFalsePositiveRate = config.bloomFilterFalsePositiveRate;
            this.blockCodec = config.blockCodec;
            this.blockCacheMaxBytes = config.blockCacheMaxBytes;
            this.targetUncompressedSize = config.targetUncompressedSize;
            this.maxConcurrentCompactions = config.maxConcurrentCompactions;
            this.l0CompactionTrigger = config.l0CompactionTrigger;
            this.maxBytesForLevelBase = config.maxBytesForLevelBase;
            this.levelMultiplier = config.levelMultiplier;
            this.maxLevels = config.maxLevels;
        }

        public Builder memtableMaxSizeBytes(long memtableMaxSizeBytes) {
            this.memtableMaxSizeBytes = memtableMaxSizeBytes;
            return this;
        }

        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder blockRestartInterval(int blockRestartInterval) {
            this.blockRestartInterval = blockRestartInterval;
            return this;
        }

        public Builder bloomFilterFalsePositiveRate(double bloomFilterFalsePositiveRate) {
            this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
            return this;
        }

        public Builder blockCodec(BlockCodec blockCodec) {
            this.blockCodec = Objects.requireNonNull(blockCodec, "blockCodec must not be null");
            return this;
        }

        public Builder blockCacheMaxBytes(long blockCacheMaxBytes) {
            this.blockCacheMaxBytes = blockCacheMaxBytes;
            return this;
        }

        public Builder targetUncompressedSize(long targetUncompressedSize) {
            this.targetUncompressedSize = targetUncompressedSize;
            return this;
        }

        public Builder maxConcurrentCompactions(int maxConcurrentCompactions) {
            this.maxConcurrentCompactions = maxConcurrentCompactions;
            return this;
        }

        public Builder l0CompactionTrigger(int l0CompactionTrigger) {
            this.l0CompactionTrigger = l0CompactionTrigger;
            return this;
        }

        public Builder maxBytesForLevelBase(long maxBytesForLevelBase) {
            this.maxBytesForLevelBase = maxBytesForLevelBase;
            return this;
        }

        public Builder levelMultiplier(int levelMultiplier) {
            this.levelMultiplier = levelMultiplier;
            return this;
        }

        public Builder maxLevels(int maxLevels) {
            this.maxLevels = maxLevels;
            return this;
        }

        public LsmConfig build() {
            return new LsmConfig(
                memtableMaxSizeBytes,
                blockSize,
                blockRestartInterval,
                bloomFilterFalsePositiveRate,
                blockCodec,
                blockCacheMaxBytes,
                targetUncompressedSize,
                maxConcurrentCompactions,
                l0CompactionTrigger,
                maxBytesForLevelBase,
                levelMultiplier,
                maxLevels
            );
        }
    }
}
