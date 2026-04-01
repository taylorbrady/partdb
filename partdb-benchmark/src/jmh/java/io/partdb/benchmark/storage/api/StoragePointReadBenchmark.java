package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageOptions;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class StoragePointReadBenchmark {

    @Benchmark
    public void hotHit(HotReadState state, KeyCursor cursor, Blackhole bh) {
        bh.consume(state.store().get(state.existingKeys[cursor.nextHit(state.existingKeys.length)]));
    }

    @Benchmark
    public void hotMiss(HotReadState state, KeyCursor cursor, Blackhole bh) {
        bh.consume(state.store().get(state.missingKeys[cursor.nextMiss(state.missingKeys.length)]));
    }

    @Benchmark
    public void persistedHit(PersistedReadState state, KeyCursor cursor, Blackhole bh) {
        bh.consume(state.store().get(state.existingKeys[cursor.nextHit(state.existingKeys.length)]));
    }

    @Benchmark
    public void persistedMiss(PersistedReadState state, KeyCursor cursor, Blackhole bh) {
        bh.consume(state.store().get(state.missingKeys[cursor.nextMiss(state.missingKeys.length)]));
    }

    @State(Scope.Thread)
    public static class KeyCursor {
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

    public abstract static class BaseReadState extends AbstractStorageState {
        Bytes[] existingKeys;
        Bytes[] missingKeys;
        private StorageOptions options;

        void prepare(int keyCount, int valueSize, boolean reopenAfterLoad) throws IOException {
            options = StorageFixtures.defaultOptions();
            existingKeys = BenchmarkKeys.storageKeys(keyCount);
            missingKeys = BenchmarkKeys.missingKeys(keyCount);

            openStore("partdb-point-read", options);
            StorageFixtures.populate(
                store(),
                existingKeys,
                BenchmarkValues.fixedValue(valueSize, 0x5eedL),
                1
            );
            store().checkpoint();
            if (reopenAfterLoad) {
                reopenStore(options);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class HotReadState extends BaseReadState {
        @Param({"10000", "100000"})
        int keyCount;

        @Param({"100", "1024"})
        int valueSize;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            prepare(keyCount, valueSize, false);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeAndDelete();
        }
    }

    @State(Scope.Benchmark)
    public static class PersistedReadState extends BaseReadState {
        @Param({"10000", "100000"})
        int keyCount;

        @Param({"100", "1024"})
        int valueSize;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            prepare(keyCount, valueSize, true);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeAndDelete();
        }
    }
}
