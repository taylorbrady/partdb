package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.Mutation;
import io.partdb.storage.Revision;
import io.partdb.storage.StorageCheckpoint;
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
        state.store().apply(
            new Revision(state.nextRevision()),
            Mutation.put(state.nextSequentialInsertKey(), state.valueTemplate)
        );
    }

    @Benchmark
    public void freshRandomInsert(FreshWriteState state) {
        state.store().apply(
            new Revision(state.nextRevision()),
            Mutation.put(state.nextRandomKey(), state.valueTemplate)
        );
    }

    @Benchmark
    public void steadyStateAppend(SteadyStateWriteState state) {
        state.store().apply(
            new Revision(state.nextRevision()),
            Mutation.put(state.nextAppendKey(), state.valueTemplate)
        );
    }

    @Benchmark
    public void steadyStateUpdateRandom(SteadyStateWriteState state) {
        state.store().apply(
            new Revision(state.nextRevision()),
            Mutation.put(state.nextRandomExistingKey(), state.valueTemplate)
        );
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

        static Bytes nextKey(Bytes[] keys, int index, String label) {
            if (index >= keys.length) {
                throw new IllegalStateException("Benchmark exhausted precomputed " + label + " keys");
            }
            return keys[index];
        }
    }

    @State(Scope.Benchmark)
    public static class FreshWriteState extends BaseWriteState {
        @Param({"100", "1024", "4096"})
        int valueSize;

        @Param({"2000000"})
        int sequentialKeySpace;

        Bytes[] sequentialKeys;
        int nextSequentialKeyIndex;

        @Setup(Level.Trial)
        public void prepareTrial() {
            prepareDataset(valueSize);
            sequentialKeys = BenchmarkKeys.storageKeys(sequentialKeySpace);
        }

        @Setup(Level.Iteration)
        public void openFreshStore() throws IOException {
            openStore("partdb-fresh-write", StorageFixtures.defaultOptions());
            revisionCounter = 0;
            randomIndex = 0;
            nextSequentialKeyIndex = 0;
        }

        @TearDown(Level.Iteration)
        public void tearDownIteration() throws IOException {
            closeAndDelete();
        }

        Bytes nextSequentialInsertKey() {
            return nextKey(sequentialKeys, nextSequentialKeyIndex++, "fresh sequential insert");
        }
    }

    @State(Scope.Benchmark)
    public static class SteadyStateWriteState extends BaseWriteState {
        @Param({"100", "1024", "4096"})
        int valueSize;

        @Param({"100000"})
        int initialKeyCount;

        Bytes[] existingKeys;
        @Param({"2000000"})
        int appendKeySpace;

        Bytes[] appendKeys;
        int nextAppendKeyIndex;
        StorageOptions options;
        StorageCheckpoint baselineCheckpoint;
        long baselineRevision;

        @Setup(Level.Trial)
        public void prepareTrial() throws IOException {
            prepareDataset(valueSize);
            options = StorageFixtures.defaultOptions();
            existingKeys = BenchmarkKeys.storageKeys(initialKeyCount);
            appendKeys = BenchmarkKeys.storageKeys(initialKeyCount, appendKeySpace);
            openStore("partdb-steady-write-baseline", options);
            baselineRevision = StorageFixtures.populate(store(), existingKeys, valueTemplate, 1) - 1;
            baselineCheckpoint = store().checkpoint();
            closeAndDelete();
        }

        @Setup(Level.Iteration)
        public void openIterationStore() throws IOException {
            openStore("partdb-steady-write", options);
            store().restoreInPlace(baselineCheckpoint);
            revisionCounter = baselineRevision;
            nextAppendKeyIndex = 0;
            randomIndex = 0;
        }

        Bytes nextRandomExistingKey() {
            Bytes key = existingKeys[randomIndex % existingKeys.length];
            randomIndex = (randomIndex + 1) % existingKeys.length;
            return key;
        }

        Bytes nextAppendKey() {
            return nextKey(appendKeys, nextAppendKeyIndex++, "steady-state append");
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeAndDelete();
        }

        @TearDown(Level.Iteration)
        public void tearDownIteration() throws IOException {
            closeAndDelete();
        }
    }
}
