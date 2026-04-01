package io.partdb.storage;

sealed interface BlockCache permits S3FifoBlockCache, NoOpBlockCache {

    DataBlockReader get(long sstableId, long offset);

    void put(long sstableId, long offset, DataBlockReader block);

    void invalidate(long sstableId);

    Stats stats();

    record Stats(long hits, long misses, long evictions, long sizeInBytes, long maxSizeInBytes) {

        public double hitRate() {
            long total = hits + misses;
            return total == 0 ? 0.0 : (double) hits / total;
        }
    }
}
