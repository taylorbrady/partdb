package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class BlockCacheTest {

    private static final long CAPACITY = 1024 * 1024;

    @Test
    void returnsCachedBlocksAndTracksSize() {
        BlockCache cache = new BlockCache(CAPACITY);
        Block block = block("alpha", 128);

        assertNull(cache.get(1, 10));
        cache.put(1, 10, block);

        assertSame(block, cache.get(1, 10));
        assertEquals(block.sizeBytes(), cache.sizeBytes());
    }

    @Test
    void invalidateRemovesOnlyThatTable() {
        BlockCache cache = new BlockCache(CAPACITY);
        Block left = block("alpha", 64);
        Block right = block("beta", 64);
        cache.put(1, 10, left);
        cache.put(2, 20, right);

        cache.invalidate(1);

        assertNull(cache.get(1, 10));
        assertSame(right, cache.get(2, 20));
        assertEquals(right.sizeBytes(), cache.sizeBytes());
    }

    @Test
    void frequentlyReadBlockSurvivesSmallQueueEviction() {
        BlockCache cache = new BlockCache(CAPACITY);
        Block hot = block("hot", 350_000);
        Block cold = block("cold", 350_000);
        Block newest = block("newest", 350_000);

        cache.put(1, 10, hot);
        assertSame(hot, cache.get(1, 10));
        cache.put(1, 20, cold);
        cache.put(1, 30, newest);

        assertSame(hot, cache.get(1, 10));
        assertNull(cache.get(1, 20));
        assertSame(newest, cache.get(1, 30));
    }

    @Test
    void zeroCapacityAndOversizedBlocksAreNotCached() {
        BlockCache disabled = new BlockCache(0);
        disabled.put(1, 10, block("a", 16));
        assertNull(disabled.get(1, 10));

        BlockCache cache = new BlockCache(CAPACITY);
        cache.put(1, 10, block("huge", (int) CAPACITY + 1));
        assertNull(cache.get(1, 10));
        assertEquals(0, cache.sizeBytes());
    }

    private static Block block(String key, int valueSize) {
        BlockWriter writer = new BlockWriter(4);
        writer.add(new InternalEntry.Value(new InternalKey(Slice.utf8(key), 1), Slice.wrap(new byte[valueSize])));
        return Block.from(MemorySegment.ofArray(writer.finish()));
    }
}
