package io.partdb.storage.sstable;

import java.nio.file.Path;
import java.util.Optional;

public interface BlockCache {

    Optional<Block> get(Path sstable, BlockHandle handle);

    void put(Path sstable, BlockHandle handle, Block block);

    void invalidate(Path sstable);

    Stats stats();

    record Stats(long hits, long misses, long evictions, long sizeInBytes, long maxSizeInBytes) {

        public double hitRate() {
            long total = hits + misses;
            return total == 0 ? 0.0 : (double) hits / total;
        }
    }
}
