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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class DataBlockReadBenchmark {

    @Benchmark
    public Optional<StoredEntry> findFirst(BlockState state) {
        return state.reader.find(state.firstKey);
    }

    @Benchmark
    public Optional<StoredEntry> findMiddle(BlockState state) {
        return state.reader.find(state.middleKey);
    }

    @Benchmark
    public Optional<StoredEntry> findLast(BlockState state) {
        return state.reader.find(state.lastKey);
    }

    @Benchmark
    public Optional<StoredEntry> findMissBefore(BlockState state) {
        return state.reader.find(state.beforeFirstKey);
    }

    @Benchmark
    public Optional<StoredEntry> findMissAfter(BlockState state) {
        return state.reader.find(state.afterLastKey);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public int scanFull(BlockState state) {
        int count = 0;
        DataBlockCursor cursor = state.reader.cursor();
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        return count;
    }

    @State(Scope.Benchmark)
    public static class BlockState {
        @Param({"256", "1024"})
        int entryCount;

        @Param({"32", "256"})
        int valueSize;

        @Param({"1", "16"})
        int restartInterval;

        @Param({"SHARED_PREFIX", "RANDOMISH"})
        String keyShape;

        DataBlockReader reader;
        Slice firstKey;
        Slice middleKey;
        Slice lastKey;
        Slice beforeFirstKey;
        Slice afterLastKey;

        @Setup(Level.Trial)
        public void setup() {
            Slice[] keys = StorageBenchmarkSupport.keys(keyShape, entryCount, 0);
            reader = StorageBenchmarkSupport.dataBlockReader(keyShape, entryCount, valueSize, restartInterval);
            firstKey = keys[0];
            middleKey = keys[entryCount / 2];
            lastKey = keys[entryCount - 1];
            beforeFirstKey = Slice.empty();
            afterLastKey = StorageBenchmarkSupport.strictlyAfter(lastKey);
        }
    }
}
