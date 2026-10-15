package io.partdb.storage.internal;

sealed interface BlockCache permits S3FifoBlockCache, NoOpBlockCache {

    DataBlockReader get(long cacheId, long offset);

    void put(long cacheId, long offset, DataBlockReader block);

    void invalidate(long cacheId);

    Stats stats();

    record Stats(long hits, long misses, long evictions, long sizeInBytes, long maxSizeInBytes) {}
}
