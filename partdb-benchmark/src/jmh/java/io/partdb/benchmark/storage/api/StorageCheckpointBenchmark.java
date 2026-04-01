package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class StorageCheckpointBenchmark {

    @Benchmark
    public void checkpoint(CheckpointState state, Blackhole bh) {
        bh.consume(state.store().checkpoint());
    }

    @State(Scope.Benchmark)
    public static class CheckpointState extends AbstractStorageState {
        @Param({"10000", "100000"})
        int keyCount;

        @Param({"100", "1024"})
        int valueSize;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            Bytes[] keys = BenchmarkKeys.storageKeys(keyCount);
            openStore("partdb-checkpoint", StorageFixtures.defaultConfig());
            StorageFixtures.populate(store(), keys, BenchmarkValues.fixedValue(valueSize, 0x8eedL), 1);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeAndDelete();
        }
    }
}
