package io.partdb.storage.sstable;

public record SSTableConfig(int blockSize, double bloomFilterFalsePositiveRate) {

    public static final int DEFAULT_BLOCK_SIZE = 8 * 1024;
    public static final double DEFAULT_BLOOM_FILTER_FPR = 0.01;

    public SSTableConfig {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }
        if (bloomFilterFalsePositiveRate <= 0 || bloomFilterFalsePositiveRate >= 1) {
            throw new IllegalArgumentException("bloomFilterFalsePositiveRate must be between 0 and 1");
        }
    }

    public static SSTableConfig create() {
        return new SSTableConfig(DEFAULT_BLOCK_SIZE, DEFAULT_BLOOM_FILTER_FPR);
    }
}
