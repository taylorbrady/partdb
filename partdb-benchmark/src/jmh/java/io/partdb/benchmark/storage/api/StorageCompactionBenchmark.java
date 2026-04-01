package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.LsmStats;
import io.partdb.storage.Mutation;
import io.partdb.storage.Revision;
import io.partdb.storage.SstableOptions;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
public class StorageCompactionBenchmark {

    @Benchmark
    public void flushToL0Burst(CompactionState state, Blackhole bh) {
        state.writeBurst();
        bh.consume(state.store().stats());
    }

    @Benchmark
    public void burstAndCatchUp(CompactionState state, Blackhole bh) {
        long completedBefore = state.store().stats().completedCompactions();
        state.writeBurst();
        bh.consume(state.awaitCompactionCatchup(completedBefore));
    }

    @State(Scope.Benchmark)
    public static class CompactionState extends AbstractStorageState {
        private static final long QUIESCE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
        private static final long POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(250);

        @Param({"100", "4096"})
        int valueSize;

        @Param({"NONE", "DEFLATE"})
        String compressionName;

        @Param({"FIXED_REPEAT", "UNIQUE_RANDOM"})
        ValuePattern valuePattern;

        @Param({"2097152"})
        int burstPayloadBytes;

        Bytes[] burstKeys;
        Bytes[] burstValues;
        long revisionCounter;
        private StorageOptions options;

        @Setup(Level.Trial)
        public void prepareTrial() {
            options = StorageFixtures.compactionOptions(SstableOptions.Compression.valueOf(compressionName));
            burstKeys = BenchmarkKeys.storageKeys(entryCountForPayloadTarget(valueSize, burstPayloadBytes));
            burstValues = buildBurstValues(valueSize, valuePattern, burstKeys.length);
        }

        @Setup(Level.Invocation)
        public void openIterationStore() throws IOException {
            openStore("partdb-compaction", options);
            revisionCounter = 0;
        }

        @TearDown(Level.Invocation)
        public void tearDownIteration() throws IOException {
            closeAndDelete();
        }

        void writeBurst() {
            for (int index = 0; index < burstKeys.length; index++) {
                store().apply(
                    new Revision(++revisionCounter),
                    Mutation.put(burstKeys[index], burstValues[index])
                );
            }
        }

        LsmStats awaitCompactionCatchup(long completedBefore) {
            long deadline = System.nanoTime() + QUIESCE_TIMEOUT_NANOS;
            boolean sawCompaction = false;

            while (System.nanoTime() < deadline) {
                LsmStats stats = store().stats();
                if (stats.activeCompactions() > 0 || stats.completedCompactions() > completedBefore) {
                    sawCompaction = true;
                }
                if (sawCompaction && stats.activeCompactions() == 0 && stats.immutableMemtableCount() == 0) {
                    return stats;
                }
                LockSupport.parkNanos(POLL_NANOS);
            }

            throw new IllegalStateException("Timed out waiting for compaction catch-up");
        }

        private static int entryCountForPayloadTarget(int valueSize, int burstPayloadBytes) {
            int entryCount = Math.max(256, Math.floorDiv(burstPayloadBytes, valueSize));
            return Math.max(1, entryCount);
        }

        private static Bytes[] buildBurstValues(int valueSize, ValuePattern valuePattern, int entryCount) {
            return switch (valuePattern) {
                case FIXED_REPEAT -> repeatedValues(valueSize, entryCount);
                case UNIQUE_RANDOM -> uniqueValues(valueSize, entryCount);
            };
        }

        private static Bytes[] repeatedValues(int valueSize, int entryCount) {
            Bytes value = BenchmarkValues.fixedValue(valueSize, 0xc011L + valueSize);
            Bytes[] values = new Bytes[entryCount];
            Arrays.fill(values, value);
            return values;
        }

        private static Bytes[] uniqueValues(int valueSize, int entryCount) {
            Bytes[] values = new Bytes[entryCount];
            long seedBase = 0xc011L + valueSize;
            for (int index = 0; index < entryCount; index++) {
                values[index] = BenchmarkValues.fixedValue(valueSize, seedBase + index);
            }
            return values;
        }
    }

    public enum ValuePattern {
        FIXED_REPEAT,
        UNIQUE_RANDOM
    }
}
