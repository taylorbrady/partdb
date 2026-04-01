package io.partdb.storage;

public record CompactionOptions(
    long targetTableSizeBytes,
    int maxConcurrentCompactions,
    int l0CompactionTrigger,
    long maxBytesForLevelBase,
    int levelMultiplier,
    int maxLevels
) {

    public CompactionOptions {
        if (targetTableSizeBytes <= 0) {
            throw new IllegalArgumentException("targetTableSizeBytes must be positive");
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

    public static CompactionOptions defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private long targetTableSizeBytes = LsmConfig.DEFAULT_TARGET_UNCOMPRESSED_SIZE;
        private int maxConcurrentCompactions = LsmConfig.DEFAULT_MAX_CONCURRENT_COMPACTIONS;
        private int l0CompactionTrigger = LsmConfig.DEFAULT_L0_COMPACTION_TRIGGER;
        private long maxBytesForLevelBase = LsmConfig.DEFAULT_MAX_BYTES_FOR_LEVEL_BASE;
        private int levelMultiplier = LsmConfig.DEFAULT_LEVEL_MULTIPLIER;
        private int maxLevels = LsmConfig.DEFAULT_MAX_LEVELS;

        private Builder() {
        }

        private Builder(CompactionOptions options) {
            this.targetTableSizeBytes = options.targetTableSizeBytes;
            this.maxConcurrentCompactions = options.maxConcurrentCompactions;
            this.l0CompactionTrigger = options.l0CompactionTrigger;
            this.maxBytesForLevelBase = options.maxBytesForLevelBase;
            this.levelMultiplier = options.levelMultiplier;
            this.maxLevels = options.maxLevels;
        }

        public Builder targetTableSizeBytes(long targetTableSizeBytes) {
            this.targetTableSizeBytes = targetTableSizeBytes;
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

        public CompactionOptions build() {
            return new CompactionOptions(
                targetTableSizeBytes,
                maxConcurrentCompactions,
                l0CompactionTrigger,
                maxBytesForLevelBase,
                levelMultiplier,
                maxLevels
            );
        }
    }
}
