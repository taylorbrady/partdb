package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageConfig;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class StorageWriteBenchmark {

    private static final int RANDOM_KEY_SPACE = 250_000;

    @Benchmark
    public void freshSequentialInsert(FreshWriteState state) {
        state.store().put(BenchmarkKeys.storageKey(state.nextSequentialKey++), state.valueTemplate, state.nextRevision());
    }

    @Benchmark
    public void freshRandomInsert(FreshWriteState state) {
        state.store().put(state.nextRandomKey(), state.valueTemplate, state.nextRevision());
    }

    @Benchmark
    public void steadyStateAppend(SteadyStateWriteState state) {
        state.store().put(BenchmarkKeys.storageKey(state.nextAppendKey++), state.valueTemplate, state.nextRevision());
    }

    @Benchmark
    public void steadyStateUpdateRandom(SteadyStateWriteState state) {
        state.store().put(state.nextRandomExistingKey(), state.valueTemplate, state.nextRevision());
    }

    public abstract static class BaseWriteState extends AbstractStorageState {
        Bytes valueTemplate;
        Bytes[] randomKeys;
        long revisionCounter;
        int randomIndex;

        void prepareDataset(int valueSize) {
            valueTemplate = BenchmarkValues.fixedValue(valueSize, 0x6eedL + valueSize);
            randomKeys = BenchmarkKeys.shuffledStorageKeys(RANDOM_KEY_SPACE, 0x9eedL + valueSize);
        }

        long nextRevision() {
            return ++revisionCounter;
        }

        Bytes nextRandomKey() {
            Bytes key = randomKeys[randomIndex];
            randomIndex = (randomIndex + 1) % randomKeys.length;
            return key;
        }
    }

    @State(Scope.Benchmark)
    public static class FreshWriteState extends BaseWriteState {
        @Param({"100", "1024", "4096"})
        int valueSize;

        long nextSequentialKey;

        @Setup(Level.Trial)
        public void prepareTrial() {
            prepareDataset(valueSize);
        }

        @Setup(Level.Iteration)
        public void openFreshStore() throws IOException {
            openStore("partdb-fresh-write", StorageFixtures.defaultConfig());
            revisionCounter = 0;
            randomIndex = 0;
            nextSequentialKey = 0;
        }

        @TearDown(Level.Iteration)
        public void tearDownIteration() throws IOException {
            closeAndDelete();
        }
    }

    @State(Scope.Benchmark)
    public static class SteadyStateWriteState extends BaseWriteState {
        @Param({"100", "1024", "4096"})
        int valueSize;

        @Param({"100000"})
        int initialKeyCount;

        Bytes[] existingKeys;
        long nextAppendKey;

        @Setup(Level.Trial)
        public void openSteadyStateStore() throws IOException {
            prepareDataset(valueSize);
            openStore("partdb-steady-write", StorageFixtures.defaultConfig());
            existingKeys = BenchmarkKeys.storageKeys(initialKeyCount);
            revisionCounter = StorageFixtures.populate(store(), existingKeys, valueTemplate, 1) - 1;
            store().checkpoint();
            nextAppendKey = initialKeyCount;
            randomIndex = 0;
        }

        Bytes nextRandomExistingKey() {
            Bytes key = existingKeys[randomIndex % existingKeys.length];
            randomIndex = (randomIndex + 1) % existingKeys.length;
            return key;
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeAndDelete();
        }
    }
}
