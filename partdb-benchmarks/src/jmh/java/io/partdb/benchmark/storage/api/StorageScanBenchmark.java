package io.partdb.benchmark.storage.api;

import io.partdb.benchmark.support.AbstractStorageState;
import io.partdb.benchmark.support.BenchmarkKeys;
import io.partdb.benchmark.support.BenchmarkValues;
import io.partdb.benchmark.support.StorageFixtures;
import io.partdb.bytes.Bytes;
import io.partdb.storage.KeyRange;
import io.partdb.storage.Scan;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class StorageScanBenchmark {

    private static final int RANGE_SAMPLE_COUNT = 2_048;

    @Benchmark
    public void hotRange100(HotScanState state, RangeCursor cursor, Blackhole bh) {
        consumeRange(state, state.range100[cursor.next100(state.range100.length)], bh);
    }

    @Benchmark
    public void hotRange1000(HotScanState state, RangeCursor cursor, Blackhole bh) {
        consumeRange(state, state.range1000[cursor.next1000(state.range1000.length)], bh);
    }

    @Benchmark
    public void persistedRange100(PersistedScanState state, RangeCursor cursor, Blackhole bh) {
        consumeRange(state, state.range100[cursor.next100(state.range100.length)], bh);
    }

    @Benchmark
    public void persistedRange1000(PersistedScanState state, RangeCursor cursor, Blackhole bh) {
        consumeRange(state, state.range1000[cursor.next1000(state.range1000.length)], bh);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long persistedFullScan(PersistedScanState state) {
        long count = 0;
        try (Scan cursor = state.store().scan(KeyRange.all())) {
            for (var ignored : cursor) {
                count++;
            }
        }
        return count;
    }

    private static void consumeRange(BaseScanState state, ScanRange range, Blackhole bh) {
        try (Scan cursor = state.store().scan(KeyRange.between(range.start(), range.endExclusive()))) {
            for (var entry : cursor) {
                bh.consume(entry);
            }
        }
    }

    private static ScanRange[] buildRanges(Bytes[] keys, int length) {
        int maxStart = keys.length - length - 1;
        ScanRange[] ranges = new ScanRange[RANGE_SAMPLE_COUNT];
        for (int i = 0; i < ranges.length; i++) {
            int start = Math.floorMod(i * 7_919, maxStart);
            ranges[i] = new ScanRange(keys[start], keys[start + length]);
        }
        return ranges;
    }

    @State(Scope.Thread)
    public static class RangeCursor {
        private int index100;
        private int index1000;

        int next100(int length) {
            int current = index100;
            index100 = (index100 + 1) % length;
            return current;
        }

        int next1000(int length) {
            int current = index1000;
            index1000 = (index1000 + 1) % length;
            return current;
        }
    }

    public abstract static class BaseScanState extends AbstractStorageState {
        Bytes[] existingKeys;
        ScanRange[] range100;
        ScanRange[] range1000;

        void prepare(int keyCount, int valueSize, boolean reopenAfterLoad) throws IOException {
            existingKeys = BenchmarkKeys.storageKeys(keyCount);

            openStore("partdb-scan", StorageFixtures.defaultOptions());
            StorageFixtures.populate(
                store(),
                existingKeys,
                BenchmarkValues.fixedValue(valueSize, 0x7eedL),
                1
            );
            store().checkpoint();
            if (reopenAfterLoad) {
                reopenStore(StorageFixtures.defaultOptions());
            }

            range100 = buildRanges(existingKeys, 100);
            range1000 = buildRanges(existingKeys, 1_000);
        }
    }

    @State(Scope.Benchmark)
    public static class HotScanState extends BaseScanState {
        @Param({"100000"})
        int keyCount;

        @Param({"100"})
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
    public static class PersistedScanState extends BaseScanState {
        @Param({"100000"})
        int keyCount;

        @Param({"100"})
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

    private record ScanRange(Bytes start, Bytes endExclusive) {
    }
}
