package io.partdb.storage.sstable;

public sealed interface BlockCache permits S3FifoBlockCache, NoOpBlockCache {

    Block get(long sstableId, long offset);

    void put(long sstableId, long offset, Block block);

    void invalidate(long sstableId);

    Stats stats();

    record Stats(long hits, long misses, long evictions, long sizeInBytes, long maxSizeInBytes) {

        public double hitRate() {
            long total = hits + misses;
            return total == 0 ? 0.0 : (double) hits / total;
        }
    }
}
