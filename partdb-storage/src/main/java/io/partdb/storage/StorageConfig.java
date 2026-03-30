package io.partdb.storage;

import java.util.Objects;

public record StorageConfig(
    long writeBufferMaxBytes,
    long readCacheMaxBytes,
    Compression compression,
    Tuning tuning
) {

    public StorageConfig {
        if (writeBufferMaxBytes <= 0) {
            throw new IllegalArgumentException("writeBufferMaxBytes must be positive");
        }
        if (readCacheMaxBytes < 0) {
            throw new IllegalArgumentException("readCacheMaxBytes must be non-negative");
        }
        Objects.requireNonNull(compression, "compression must not be null");
        tuning = Objects.requireNonNull(tuning, "tuning must not be null");
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
            tuning.dataBlockSizeBytes(),
            tuning.bloomFilterFalsePositiveRate(),
            compression.toBlockCodec(),
            readCacheMaxBytes,
            tuning.targetTableSizeBytes(),
            tuning.maxConcurrentCompactions(),
            tuning.l0CompactionTrigger(),
            tuning.maxBytesForLevelBase(),
            tuning.levelMultiplier(),
            tuning.maxLevels()
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

    public record Tuning(
        int dataBlockSizeBytes,
        double bloomFilterFalsePositiveRate,
        long targetTableSizeBytes,
        int maxConcurrentCompactions,
        int l0CompactionTrigger,
        long maxBytesForLevelBase,
        int levelMultiplier,
        int maxLevels
    ) {

        public Tuning {
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

        public static Tuning defaults() {
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

            private Builder(Tuning tuning) {
                this.dataBlockSizeBytes = tuning.dataBlockSizeBytes;
                this.bloomFilterFalsePositiveRate = tuning.bloomFilterFalsePositiveRate;
                this.targetTableSizeBytes = tuning.targetTableSizeBytes;
                this.maxConcurrentCompactions = tuning.maxConcurrentCompactions;
                this.l0CompactionTrigger = tuning.l0CompactionTrigger;
                this.maxBytesForLevelBase = tuning.maxBytesForLevelBase;
                this.levelMultiplier = tuning.levelMultiplier;
                this.maxLevels = tuning.maxLevels;
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

            public Tuning build() {
                return new Tuning(
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
        private Tuning tuning = Tuning.defaults();

        private Builder() {
        }

        private Builder(StorageConfig config) {
            this.writeBufferMaxBytes = config.writeBufferMaxBytes;
            this.readCacheMaxBytes = config.readCacheMaxBytes;
            this.compression = config.compression;
            this.tuning = config.tuning;
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

        public Builder tuning(Tuning tuning) {
            this.tuning = Objects.requireNonNull(tuning, "tuning must not be null");
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(writeBufferMaxBytes, readCacheMaxBytes, compression, tuning);
        }
    }
}
