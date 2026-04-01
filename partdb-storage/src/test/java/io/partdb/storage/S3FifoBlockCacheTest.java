package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class S3FifoBlockCacheTest {

    private static final long MIN_CACHE_SIZE = 1_024L * 1_024L;

    @Test
    void recordsHitsMissesAndSize() {
        BlockCache cache = new S3FifoBlockCache(MIN_CACHE_SIZE);
        DataBlockReader block = block("alpha", 128);

        assertNull(cache.get(1, 10));

        cache.put(1, 10, block);

        assertSame(block, cache.get(1, 10));
        assertSame(block, cache.get(1, 10));

        BlockCache.Stats stats = cache.stats();
        assertEquals(2, stats.hits());
        assertEquals(1, stats.misses());
        assertEquals(0, stats.evictions());
        assertEquals(block.sizeInBytes(), stats.sizeInBytes());
    }

    @Test
    void invalidateRemovesOnlyMatchingSstable() {
        BlockCache cache = new S3FifoBlockCache(MIN_CACHE_SIZE);
        DataBlockReader left = block("alpha", 64);
        DataBlockReader right = block("beta", 64);

        cache.put(1, 10, left);
        cache.put(2, 20, right);
        cache.invalidate(1);

        assertNull(cache.get(1, 10));
        assertSame(right, cache.get(2, 20));
    }

    @Test
    void frequentlyReadBlockSurvivesSmallQueueEviction() {
        BlockCache cache = new S3FifoBlockCache(MIN_CACHE_SIZE);
        DataBlockReader hot = block("hot", 350_000);
        DataBlockReader cold = block("cold", 350_000);
        DataBlockReader newest = block("newest", 350_000);

        cache.put(1, 10, hot);
        assertSame(hot, cache.get(1, 10));

        cache.put(1, 20, cold);
        cache.put(1, 30, newest);

        assertSame(hot, cache.get(1, 10));
        assertNull(cache.get(1, 20));
        assertSame(newest, cache.get(1, 30));
        assertEquals(1, cache.stats().evictions());
    }

    @Test
    void skipsBlocksLargerThanCacheCapacity() {
        BlockCache cache = new S3FifoBlockCache(MIN_CACHE_SIZE);
        DataBlockReader huge = block("huge", (int) (MIN_CACHE_SIZE + 32_768));

        cache.put(1, 10, huge);

        assertNull(cache.get(1, 10));
        assertEquals(0, cache.stats().sizeInBytes());
    }

    private static DataBlockReader block(String key, int valueSize) {
        DataBlockWriter writer = new DataBlockWriter(4);
        writer.append(new StoredEntry.Value(
            Slice.utf8(key),
            Slice.copyOf(new byte[valueSize]),
            1
        ));
        return DataBlockReader.from(MemorySegment.ofArray(writer.finish()));
    }
}
