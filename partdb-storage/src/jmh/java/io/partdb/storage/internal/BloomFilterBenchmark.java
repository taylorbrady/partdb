package io.partdb.storage.internal;

import io.partdb.storage.*;

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
public class BloomFilterBenchmark {

    @Benchmark
    public boolean hit(FilterState state, ProbeCursor cursor) {
        return state.filter.mightContain(state.hitKeys[cursor.nextHit(state.hitKeys.length)]);
    }

    @Benchmark
    public boolean miss(FilterState state, ProbeCursor cursor) {
        return state.filter.mightContain(state.missKeys[cursor.nextMiss(state.missKeys.length)]);
    }

    @State(Scope.Thread)
    public static class ProbeCursor {
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
    public static class FilterState {
        private static final int SAMPLE_SIZE = 4_096;

        @Param({"10000", "100000"})
        int keyCount;

        @Param({"0.01", "0.001"})
        double falsePositiveRate;

        @Param({"SHARED_PREFIX", "RANDOMISH"})
        String keyShape;

        BloomFilter filter;
        Slice[] hitKeys;
        Slice[] missKeys;

        @Setup(Level.Trial)
        public void setup() {
            Slice[] keys = StorageBenchmarkSupport.keys(keyShape, keyCount, 0);
            filter = BloomFilter.build(StorageBenchmarkSupport.sliceList(keys), falsePositiveRate);
            hitKeys = StorageBenchmarkSupport.keys(keyShape, SAMPLE_SIZE, 0);
            missKeys = StorageBenchmarkSupport.keys(keyShape, SAMPLE_SIZE, keyCount + 1_000_000);
        }
    }
}
