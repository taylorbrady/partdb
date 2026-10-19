package io.partdb.storage;

import java.util.Objects;

public record StoreOptions(
    long memtableBytes,
    long blockCacheBytes,
    int blockBytes,
    Compression compression,
    long targetTableBytes,
    int levelZeroCompactionTrigger,
    int levelZeroStopTrigger,
    int compactionConcurrency
) {

    private static final long KIB = 1024;
    private static final long MIB = 1024 * KIB;

    public StoreOptions {
        if (memtableBytes <= 0) {
            throw new IllegalArgumentException("memtableBytes must be positive");
        }
        if (blockCacheBytes < 0) {
            throw new IllegalArgumentException("blockCacheBytes must not be negative");
        }
        if (blockBytes <= 0) {
            throw new IllegalArgumentException("blockBytes must be positive");
        }
        Objects.requireNonNull(compression, "compression must not be null");
        if (targetTableBytes <= 0) {
            throw new IllegalArgumentException("targetTableBytes must be positive");
        }
        if (levelZeroCompactionTrigger <= 0) {
            throw new IllegalArgumentException("levelZeroCompactionTrigger must be positive");
        }
        if (levelZeroStopTrigger <= levelZeroCompactionTrigger) {
            throw new IllegalArgumentException("levelZeroStopTrigger must be greater than levelZeroCompactionTrigger");
        }
        if (compactionConcurrency <= 0) {
            throw new IllegalArgumentException("compactionConcurrency must be positive");
        }
    }

    public static StoreOptions defaults() {
        return new StoreOptions(64 * MIB, 64 * MIB, (int) (16 * KIB), Compression.DEFLATE, 64 * MIB, 4, 12, 2);
    }

    public StoreOptions withMemtableBytes(long memtableBytes) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, levelZeroCompactionTrigger, levelZeroStopTrigger, compactionConcurrency);
    }

    public StoreOptions withBlockCacheBytes(long blockCacheBytes) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, levelZeroCompactionTrigger, levelZeroStopTrigger, compactionConcurrency);
    }

    public StoreOptions withBlockBytes(int blockBytes) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, levelZeroCompactionTrigger, levelZeroStopTrigger, compactionConcurrency);
    }

    public StoreOptions withCompression(Compression compression) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, levelZeroCompactionTrigger, levelZeroStopTrigger, compactionConcurrency);
    }

    public StoreOptions withTargetTableBytes(long targetTableBytes) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, levelZeroCompactionTrigger, levelZeroStopTrigger, compactionConcurrency);
    }

    public StoreOptions withLevelZeroTriggers(int compaction, int stop) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, compaction, stop, compactionConcurrency);
    }

    public StoreOptions withCompactionConcurrency(int compactionConcurrency) {
        return new StoreOptions(memtableBytes, blockCacheBytes, blockBytes, compression, targetTableBytes, levelZeroCompactionTrigger, levelZeroStopTrigger, compactionConcurrency);
    }

    long levelBaseBytes() {
        return memtableBytes * levelZeroCompactionTrigger;
    }

    public enum Compression {
        NONE,
        DEFLATE
    }
}
