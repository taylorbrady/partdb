package io.partdb.benchmark;

import io.partdb.storage.StateStore;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.StorageCursor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class LsmEngineScanBenchmark {

    private static final int KEY_COUNT = 100_000;
    private static final int VALUE_SIZE = 100;

    private Path tempDir;
    private StateStore store;
    private byte[][] existingKeys;
    private byte[] valueBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-scan-bench");
        store = StateStore.open(tempDir, StorageConfig.defaults());
        existingKeys = new byte[KEY_COUNT][];

        valueBytes = new byte[VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(valueBytes);

        for (int i = 0; i < KEY_COUNT; i++) {
            byte[] key = formatKey(i);
            existingKeys[i] = key;
            store.put(key, valueBytes, i);
        }

        store.snapshot();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        store.close();
        deleteDirectory(tempDir);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long scanFull() {
        long count = 0;
        try (StorageCursor cursor = store.scan(null, null)) {
            while (cursor.hasNext()) {
                cursor.next();
                count++;
            }
        }
        return count;
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(100)
    public void scanRange100(Blackhole bh) {
        int start = ThreadLocalRandom.current().nextInt(KEY_COUNT - 100);
        byte[] startKey = existingKeys[start];
        byte[] endKey = existingKeys[start + 100];
        try (StorageCursor cursor = store.scan(startKey, endKey)) {
            while (cursor.hasNext()) {
                bh.consume(cursor.next());
            }
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(1000)
    public void scanRange1000(Blackhole bh) {
        int start = ThreadLocalRandom.current().nextInt(KEY_COUNT - 1000);
        byte[] startKey = existingKeys[start];
        byte[] endKey = existingKeys[start + 1000];
        try (StorageCursor cursor = store.scan(startKey, endKey)) {
            while (cursor.hasNext()) {
                bh.consume(cursor.next());
            }
        }
    }

    private static byte[] formatKey(long keyNum) {
        return ("key" + String.format("%016d", keyNum)).getBytes(StandardCharsets.UTF_8);
    }

    private static void deleteDirectory(Path dir) throws IOException {
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException ignored) {}
                });
        }
    }
}
