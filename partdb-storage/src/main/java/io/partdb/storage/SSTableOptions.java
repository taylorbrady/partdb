package io.partdb.storage;

import java.util.Objects;

public record SSTableOptions(
    int blockSizeBytes,
    int blockRestartInterval,
    double bloomFilterFalsePositiveRate,
    Compression compression
) {

    public SSTableOptions {
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

    public static SSTableOptions defaults() {
        return builder().build();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public enum Compression {
        NONE,
        DEFLATE
    }

    public static final class Builder {
        private int blockSizeBytes = 32 * 1024;
        private int blockRestartInterval = 16;
        private double bloomFilterFalsePositiveRate = 0.01;
        private Compression compression = Compression.DEFLATE;

        private Builder() {
        }

        private Builder(SSTableOptions options) {
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

        public SSTableOptions build() {
            return new SSTableOptions(
                blockSizeBytes,
                blockRestartInterval,
                bloomFilterFalsePositiveRate,
                compression
            );
        }
    }
}
