package io.partdb.storage.sstable;

import io.partdb.storage.compaction.CompactionStrategy;
import io.partdb.storage.compaction.LeveledCompactionStrategy;

import java.util.Objects;

public record SSTableConfig(
    int blockSize,
    double bloomFilterFalsePositiveRate,
    BlockCodec codec,
    long blockCacheMaxBytes,
    long targetUncompressedSize,
    int maxConcurrentCompactions,
    int l0CompactionTrigger,
    long maxBytesForLevelBase,
    int levelMultiplier,
    int maxLevels
) {

    public static final int DEFAULT_BLOCK_SIZE = 32 * 1024;
    public static final double DEFAULT_BLOOM_FILTER_FPR = 0.01;
    public static final long DEFAULT_BLOCK_CACHE_MAX_BYTES = 64 * 1024 * 1024;
    public static final long DEFAULT_TARGET_UNCOMPRESSED_SIZE = 64 * 1024 * 1024;
    public static final int DEFAULT_MAX_CONCURRENT = 4;
    public static final int DEFAULT_L0_COMPACTION_TRIGGER = 4;
    public static final long DEFAULT_MAX_BYTES_FOR_LEVEL_BASE = 10 * 1024 * 1024;
    public static final int DEFAULT_LEVEL_MULTIPLIER = 10;
    public static final int DEFAULT_MAX_LEVELS = 7;

    public SSTableConfig {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }
        if (bloomFilterFalsePositiveRate <= 0 || bloomFilterFalsePositiveRate >= 1) {
            throw new IllegalArgumentException("bloomFilterFalsePositiveRate must be between 0 and 1");
        }
        Objects.requireNonNull(codec, "codec");
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

    public static SSTableConfig defaults() {
        return new SSTableConfig(
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOOM_FILTER_FPR,
            BlockCodec.LZ4,
            DEFAULT_BLOCK_CACHE_MAX_BYTES,
            DEFAULT_TARGET_UNCOMPRESSED_SIZE,
            DEFAULT_MAX_CONCURRENT,
            DEFAULT_L0_COMPACTION_TRIGGER,
            DEFAULT_MAX_BYTES_FOR_LEVEL_BASE,
            DEFAULT_LEVEL_MULTIPLIER,
            DEFAULT_MAX_LEVELS
        );
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

    CompactionStrategy createStrategy() {
        return new LeveledCompactionStrategy(this);
    }
}
