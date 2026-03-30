package io.partdb.storage;

import java.util.Objects;

public record StorageConfig(
    long writeBufferMaxBytes,
    long readCacheMaxBytes,
    Compression compression,
    AdvancedTuning advancedTuning
) {

    public StorageConfig {
        if (writeBufferMaxBytes <= 0) {
            throw new IllegalArgumentException("writeBufferMaxBytes must be positive");
        }
        if (readCacheMaxBytes < 0) {
            throw new IllegalArgumentException("readCacheMaxBytes must be non-negative");
        }
        Objects.requireNonNull(compression, "compression must not be null");
        advancedTuning = Objects.requireNonNull(advancedTuning, "advancedTuning must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static StorageConfig defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    LsmConfig toLsmConfig() {
        return new LsmConfig(
            writeBufferMaxBytes,
            advancedTuning.dataBlockSizeBytes(),
            advancedTuning.bloomFilterFalsePositiveRate(),
            compression.toBlockCodec(),
            readCacheMaxBytes,
            advancedTuning.targetTableSizeBytes(),
            advancedTuning.maxConcurrentCompactions(),
            advancedTuning.l0CompactionTrigger(),
            advancedTuning.maxBytesForLevelBase(),
            advancedTuning.levelMultiplier(),
            advancedTuning.maxLevels()
        );
    }

    public enum Compression {
        NONE,
        DEFLATE;

        BlockCodec toBlockCodec() {
            return switch (this) {
                case NONE -> BlockCodec.NONE;
                case DEFLATE -> BlockCodec.DEFLATE;
            };
        }
    }

    public record AdvancedTuning(
        int dataBlockSizeBytes,
        double bloomFilterFalsePositiveRate,
        long targetTableSizeBytes,
        int maxConcurrentCompactions,
        int l0CompactionTrigger,
        long maxBytesForLevelBase,
        int levelMultiplier,
        int maxLevels
    ) {

        public AdvancedTuning {
            if (dataBlockSizeBytes <= 0) {
                throw new IllegalArgumentException("dataBlockSizeBytes must be positive");
            }
            if (bloomFilterFalsePositiveRate <= 0 || bloomFilterFalsePositiveRate >= 1) {
                throw new IllegalArgumentException("bloomFilterFalsePositiveRate must be between 0 and 1");
            }
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

        public static AdvancedTuning defaults() {
            return builder().build();
        }

        public Builder toBuilder() {
            return new Builder(this);
        }

        public static final class Builder {
            private int dataBlockSizeBytes = LsmConfig.DEFAULT_BLOCK_SIZE;
            private double bloomFilterFalsePositiveRate = LsmConfig.DEFAULT_BLOOM_FILTER_FPR;
            private long targetTableSizeBytes = LsmConfig.DEFAULT_TARGET_UNCOMPRESSED_SIZE;
            private int maxConcurrentCompactions = LsmConfig.DEFAULT_MAX_CONCURRENT_COMPACTIONS;
            private int l0CompactionTrigger = LsmConfig.DEFAULT_L0_COMPACTION_TRIGGER;
            private long maxBytesForLevelBase = LsmConfig.DEFAULT_MAX_BYTES_FOR_LEVEL_BASE;
            private int levelMultiplier = LsmConfig.DEFAULT_LEVEL_MULTIPLIER;
            private int maxLevels = LsmConfig.DEFAULT_MAX_LEVELS;

            private Builder() {
            }

            private Builder(AdvancedTuning advancedTuning) {
                this.dataBlockSizeBytes = advancedTuning.dataBlockSizeBytes;
                this.bloomFilterFalsePositiveRate = advancedTuning.bloomFilterFalsePositiveRate;
                this.targetTableSizeBytes = advancedTuning.targetTableSizeBytes;
                this.maxConcurrentCompactions = advancedTuning.maxConcurrentCompactions;
                this.l0CompactionTrigger = advancedTuning.l0CompactionTrigger;
                this.maxBytesForLevelBase = advancedTuning.maxBytesForLevelBase;
                this.levelMultiplier = advancedTuning.levelMultiplier;
                this.maxLevels = advancedTuning.maxLevels;
            }

            public Builder dataBlockSizeBytes(int dataBlockSizeBytes) {
                this.dataBlockSizeBytes = dataBlockSizeBytes;
                return this;
            }

            public Builder bloomFilterFalsePositiveRate(double bloomFilterFalsePositiveRate) {
                this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
                return this;
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

            public AdvancedTuning build() {
                return new AdvancedTuning(
                    dataBlockSizeBytes,
                    bloomFilterFalsePositiveRate,
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

    public static final class Builder {
        private long writeBufferMaxBytes = LsmConfig.DEFAULT_MEMTABLE_MAX_SIZE_BYTES;
        private long readCacheMaxBytes = LsmConfig.DEFAULT_BLOCK_CACHE_MAX_BYTES;
        private Compression compression = Compression.DEFLATE;
        private AdvancedTuning advancedTuning = AdvancedTuning.defaults();

        private Builder() {
        }

        private Builder(StorageConfig config) {
            this.writeBufferMaxBytes = config.writeBufferMaxBytes;
            this.readCacheMaxBytes = config.readCacheMaxBytes;
            this.compression = config.compression;
            this.advancedTuning = config.advancedTuning;
        }

        public Builder writeBufferMaxBytes(long writeBufferMaxBytes) {
            this.writeBufferMaxBytes = writeBufferMaxBytes;
            return this;
        }

        public Builder readCacheMaxBytes(long readCacheMaxBytes) {
            this.readCacheMaxBytes = readCacheMaxBytes;
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = Objects.requireNonNull(compression, "compression must not be null");
            return this;
        }

        public Builder advancedTuning(AdvancedTuning advancedTuning) {
            this.advancedTuning = Objects.requireNonNull(advancedTuning, "advancedTuning must not be null");
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(writeBufferMaxBytes, readCacheMaxBytes, compression, advancedTuning);
        }
    }
}
