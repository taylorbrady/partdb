package io.partdb.storage.compaction;

public record LeveledCompactionConfig(
    int l0CompactionTrigger,
    long maxBytesForLevelBase,
    int levelMultiplier,
    int maxLevels
) {

    public LeveledCompactionConfig {
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

    public static LeveledCompactionConfig create() {
        return new LeveledCompactionConfig(
            4,
            10 * 1024 * 1024,
            10,
            7
        );
    }

    public long maxBytesForLevel(int level) {
        if (level == 0) {
            return Long.MAX_VALUE;
        }
        return maxBytesForLevelBase * (long) Math.pow(levelMultiplier, level - 1);
    }
}
