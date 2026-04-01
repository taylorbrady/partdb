package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.benchmark.support.StorageWorkloadScript;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
public class StorageWorkloadBenchmark {

    @Benchmark
    @Threads(1)
    public void scriptedSingleThreaded(WorkloadState state, Blackhole bh) {
        execute(state, bh);
    }

    @Benchmark
    @Threads(4)
    public void scriptedMultiThreaded(WorkloadState state, Blackhole bh) {
        execute(state, bh);
    }

    private static void execute(WorkloadState state, Blackhole bh) {
        StorageWorkloadScript.Operation operation = state.nextOperation();
        switch (operation.kind()) {
            case READ_EXISTING, READ_MISSING -> bh.consume(state.store().get(operation.key()));
            case UPDATE_EXISTING, INSERT_NEW -> state.store().put(operation.key(), state.valueTemplate, state.nextRevision());
        }
    }

    @State(Scope.Benchmark)
    public static class WorkloadState extends AbstractStorageState {
        @Param({"50", "95"})
        int readPercent;

        @Param({"100000"})
        int initialKeyCount;

        @Param({"100", "1024"})
        int valueSize;

        @Param({"100000"})
        int operationCount;

        Bytes valueTemplate;
        private StorageWorkloadScript.Operation[] operations;
        private AtomicInteger operationCursor;
        private AtomicLong revisionCounter;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            openStore("partdb-workload", StorageFixtures.defaultConfig());

            valueTemplate = BenchmarkValues.fixedValue(valueSize, 0xace1L);
            Bytes[] existingKeys = BenchmarkKeys.storageKeys(initialKeyCount);
            Bytes[] missingKeys = BenchmarkKeys.missingKeys(initialKeyCount);

            long nextRevision = StorageFixtures.populate(store(), existingKeys, valueTemplate, 1);
            store().checkpoint();

            operations = StorageWorkloadScript.mixed(
                existingKeys,
                missingKeys,
                initialKeyCount,
                operationCount,
                readPercent,
                0xbeefL + valueSize
            );
            operationCursor = new AtomicInteger();
            revisionCounter = new AtomicLong(nextRevision);
        }

        StorageWorkloadScript.Operation nextOperation() {
            int index = Math.floorMod(operationCursor.getAndIncrement(), operations.length);
            return operations[index];
        }

        long nextRevision() {
            return revisionCounter.getAndIncrement();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeAndDelete();
        }
    }
}
