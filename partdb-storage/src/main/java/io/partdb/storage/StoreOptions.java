package io.partdb.storage;

import java.util.Objects;

public record StoreOptions(long memtableMaxBytes, long blockCacheMaxBytes, Table table, Compaction compaction) {

    private static final long MIB = 1024 * 1024;

    public StoreOptions {
        if (memtableMaxBytes <= 0) {
            throw new IllegalArgumentException("memtableMaxBytes must be positive");
        }
        if (blockCacheMaxBytes < 0) {
            throw new IllegalArgumentException("blockCacheMaxBytes must not be negative");
        }
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(compaction, "compaction must not be null");
    }

    public static StoreOptions defaults() {
        return new StoreOptions(64 * MIB, 64 * MIB, Table.defaults(), Compaction.defaults());
    }

    public StoreOptions withMemtableMaxBytes(long memtableMaxBytes) {
        return new StoreOptions(memtableMaxBytes, blockCacheMaxBytes, table, compaction);
    }

    public StoreOptions withBlockCacheMaxBytes(long blockCacheMaxBytes) {
        return new StoreOptions(memtableMaxBytes, blockCacheMaxBytes, table, compaction);
    }

    public StoreOptions withTable(Table table) {
        return new StoreOptions(memtableMaxBytes, blockCacheMaxBytes, table, compaction);
    }

    public StoreOptions withCompaction(Compaction compaction) {
        return new StoreOptions(memtableMaxBytes, blockCacheMaxBytes, table, compaction);
    }

    public enum Compression {
        NONE,
        DEFLATE
    }

    public record Table(
        int blockSizeBytes,
        int blockRestartInterval,
        double bloomFalsePositiveRate,
        Compression compression
    ) {
        public Table {
            if (blockSizeBytes <= 0) {
                throw new IllegalArgumentException("blockSizeBytes must be positive");
            }
            if (blockRestartInterval <= 0) {
                throw new IllegalArgumentException("blockRestartInterval must be positive");
            }
            if (bloomFalsePositiveRate <= 0 || bloomFalsePositiveRate >= 1) {
                throw new IllegalArgumentException("bloomFalsePositiveRate must be between 0 and 1");
            }
            Objects.requireNonNull(compression, "compression must not be null");
        }

        public static Table defaults() {
            return new Table(32 * 1024, 16, 0.01, Compression.DEFLATE);
        }

        public Table withBlockSizeBytes(int blockSizeBytes) {
            return new Table(blockSizeBytes, blockRestartInterval, bloomFalsePositiveRate, compression);
        }

        public Table withCompression(Compression compression) {
            return new Table(blockSizeBytes, blockRestartInterval, bloomFalsePositiveRate, compression);
        }
    }

    public record Compaction(
        long targetTableBytes,
        int maxConcurrent,
        int levelZeroTrigger,
        long levelBaseBytes,
        int levelMultiplier,
        int maxLevels
    ) {
        public Compaction {
            if (targetTableBytes <= 0) {
                throw new IllegalArgumentException("targetTableBytes must be positive");
            }
            if (maxConcurrent <= 0) {
                throw new IllegalArgumentException("maxConcurrent must be positive");
            }
            if (levelZeroTrigger <= 0) {
                throw new IllegalArgumentException("levelZeroTrigger must be positive");
            }
            if (levelBaseBytes <= 0) {
                throw new IllegalArgumentException("levelBaseBytes must be positive");
            }
            if (levelMultiplier <= 1) {
                throw new IllegalArgumentException("levelMultiplier must be greater than 1");
            }
            if (maxLevels <= 0) {
                throw new IllegalArgumentException("maxLevels must be positive");
            }
        }

        public static Compaction defaults() {
            return new Compaction(64 * MIB, 4, 4, 10 * MIB, 10, 7);
        }

        long maxBytesForLevel(int level) {
            long bytes = levelBaseBytes;
            for (int i = 1; i < level; i++) {
                bytes = bytes > Long.MAX_VALUE / levelMultiplier ? Long.MAX_VALUE : bytes * levelMultiplier;
            }
            return bytes;
        }
    }
}
