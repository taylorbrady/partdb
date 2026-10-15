package io.partdb.storage;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class BlockCacheBenchmark {

    private static final long TABLE = 7L;
    private static final long OTHER_TABLE = 29L;

    @Benchmark
    public Block hotHit(CacheState state, OffsetCursor cursor) {
        return state.cache.get(TABLE, state.offsets[cursor.next(state.offsets.length)]);
    }

    @Benchmark
    public Block coldMiss(CacheState state, OffsetCursor cursor) {
        return state.cache.get(OTHER_TABLE, state.offsets[cursor.next(state.offsets.length)]);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long invalidateTable(InvalidateState state) {
        state.cache.invalidate(TABLE);
        return state.cache.sizeBytes();
    }

    @State(Scope.Thread)
    public static class OffsetCursor {
        private int index;

        int next(int length) {
            int current = index;
            index = (index + 1) % length;
            return current;
        }
    }

    @State(Scope.Benchmark)
    public static class CacheState {
        @Param({"1048576", "8388608"})
        long cacheSizeBytes;

        @Param({"128", "2048"})
        int blockValueSize;

        @Param({"64", "512"})
        int blockCount;

        BlockCache cache;
        long[] offsets;

        @Setup(Level.Trial)
        public void setup() {
            cache = new BlockCache(cacheSizeBytes);
            offsets = new long[blockCount];
            for (int i = 0; i < blockCount; i++) {
                offsets[i] = i * 4096L;
                cache.put(TABLE, offsets[i], BenchmarkData.cacheBlock(i, blockValueSize));
            }
        }
    }

    @State(Scope.Benchmark)
    public static class InvalidateState {
        @Param({"1048576", "8388608"})
        long cacheSizeBytes;

        @Param({"128", "2048"})
        int blockValueSize;

        @Param({"64", "512"})
        int blockCount;

        BlockCache cache;

        @Setup(Level.Invocation)
        public void setup() {
            cache = new BlockCache(cacheSizeBytes);
            for (int i = 0; i < blockCount; i++) {
                cache.put(TABLE, i * 4096L, BenchmarkData.cacheBlock(i, blockValueSize));
                cache.put(OTHER_TABLE, i * 4096L, BenchmarkData.cacheBlock(i + blockCount, blockValueSize));
            }
        }
    }
}
