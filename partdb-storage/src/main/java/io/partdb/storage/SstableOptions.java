package io.partdb.storage;

import java.util.Objects;

public record SstableOptions(
    int blockSizeBytes,
    int blockRestartInterval,
    double bloomFilterFalsePositiveRate,
    Compression compression
) {

    public SstableOptions {
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

    public static Builder builder() {
        return new Builder();
    }

    public static SstableOptions defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
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

    public static final class Builder {
        private int blockSizeBytes = LsmConfig.DEFAULT_BLOCK_SIZE;
        private int blockRestartInterval = LsmConfig.DEFAULT_BLOCK_RESTART_INTERVAL;
        private double bloomFilterFalsePositiveRate = LsmConfig.DEFAULT_BLOOM_FILTER_FPR;
        private Compression compression = Compression.DEFLATE;

        private Builder() {
        }

        private Builder(SstableOptions options) {
            this.blockSizeBytes = options.blockSizeBytes;
            this.blockRestartInterval = options.blockRestartInterval;
            this.bloomFilterFalsePositiveRate = options.bloomFilterFalsePositiveRate;
            this.compression = options.compression;
        }

        public Builder blockSizeBytes(int blockSizeBytes) {
            this.blockSizeBytes = blockSizeBytes;
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

        public Builder compression(Compression compression) {
            this.compression = Objects.requireNonNull(compression, "compression must not be null");
            return this;
        }

        public SstableOptions build() {
            return new SstableOptions(
                blockSizeBytes,
                blockRestartInterval,
                bloomFilterFalsePositiveRate,
                compression
            );
        }
    }
}
