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

    @Benchmark
    public DataBlockReader hotHit(HotCacheState state, OffsetCursor cursor) {
        return state.cache.get(state.sstableId, state.offsets[cursor.nextHit(state.offsets.length)]);
    }

    @Benchmark
    public DataBlockReader coldMiss(MissState state, OffsetCursor cursor) {
        return state.cache.get(state.sstableId, state.offsets[cursor.nextMiss(state.offsets.length)]);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long invalidateSstable(InvalidateState state) {
        state.cache.invalidate(state.targetSstableId);
        return state.cache.stats().sizeInBytes();
    }

    @State(Scope.Thread)
    public static class OffsetCursor {
        private int hitIndex;
        private int missIndex;

        int nextHit(int length) {
            int current = hitIndex;
            hitIndex = (hitIndex + 1) % length;
            return current;
        }

        int nextMiss(int length) {
            int current = missIndex;
            missIndex = (missIndex + 1) % length;
            return current;
        }
    }

    @State(Scope.Benchmark)
    public static class HotCacheState {
        @Param({"1048576", "8388608"})
        long cacheSizeBytes;

        @Param({"128", "2048"})
        int blockValueSize;

        @Param({"64", "512"})
        int blockCount;

        S3FifoBlockCache cache;
        long sstableId;
        long[] offsets;

        @Setup(Level.Trial)
        public void setup() {
            cache = new S3FifoBlockCache(cacheSizeBytes);
            sstableId = 7L;
            offsets = new long[blockCount];
            for (int i = 0; i < blockCount; i++) {
                long offset = i * 4096L;
                offsets[i] = offset;
                cache.put(sstableId, offset, StorageBenchmarkSupport.cacheBlock(i, blockValueSize));
            }
        }
    }

    @State(Scope.Benchmark)
    public static class MissState {
        @Param({"1048576", "8388608"})
        long cacheSizeBytes;

        @Param({"128", "2048"})
        int blockValueSize;

        @Param({"64", "512"})
        int blockCount;

        S3FifoBlockCache cache;
        long sstableId;
        long[] offsets;

        @Setup(Level.Trial)
        public void setup() {
            cache = new S3FifoBlockCache(cacheSizeBytes);
            sstableId = 11L;
            offsets = new long[blockCount];
            for (int i = 0; i < blockCount; i++) {
                offsets[i] = i * 4096L;
            }
        }
    }

    @State(Scope.Benchmark)
    public static class InvalidateState {
        private static final long OTHER_SSTABLE_ID = 29L;

        @Param({"1048576", "8388608"})
        long cacheSizeBytes;

        @Param({"128", "2048"})
        int blockValueSize;

        @Param({"64", "512"})
        int blockCount;

        S3FifoBlockCache cache;
        long targetSstableId;

        @Setup(Level.Invocation)
        public void setup() {
            cache = new S3FifoBlockCache(cacheSizeBytes);
            targetSstableId = 17L;
            for (int i = 0; i < blockCount; i++) {
                long offset = i * 4096L;
                cache.put(targetSstableId, offset, StorageBenchmarkSupport.cacheBlock(i, blockValueSize));
                cache.put(OTHER_SSTABLE_ID, offset, StorageBenchmarkSupport.cacheBlock(i + blockCount, blockValueSize));
            }
        }
    }
}
