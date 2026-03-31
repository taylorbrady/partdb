package io.partdb.benchmark;

import io.partdb.bytes.Bytes;
import io.partdb.storage.EntryCursor;
import io.partdb.storage.KeyRange;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedKeyValueStore;
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
public class StoreScanBenchmark {

    private static final int KEY_COUNT = 100_000;
    private static final int VALUE_SIZE = 100;

    private Path tempDir;
    private VersionedKeyValueStore store;
    private Bytes[] existingKeys;
    private Bytes valueBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-scan-bench");
        store = VersionedKeyValueStore.open(tempDir, StorageConfig.defaults());
        existingKeys = new Bytes[KEY_COUNT];

        byte[] bytes = new byte[VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(bytes);
        valueBytes = Bytes.copyOf(bytes);

        for (int i = 0; i < KEY_COUNT; i++) {
            Bytes key = formatKey(i);
            existingKeys[i] = key;
            store.put(key, valueBytes, i);
        }

        store.checkpoint();
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
        try (EntryCursor cursor = store.scan(KeyRange.all())) {
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
        Bytes startKey = existingKeys[start];
        Bytes endKey = existingKeys[start + 100];
        try (EntryCursor cursor = store.scan(KeyRange.between(startKey, endKey))) {
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
        Bytes startKey = existingKeys[start];
        Bytes endKey = existingKeys[start + 1000];
        try (EntryCursor cursor = store.scan(KeyRange.between(startKey, endKey))) {
            while (cursor.hasNext()) {
                bh.consume(cursor.next());
            }
        }
    }

    private static Bytes formatKey(long keyNum) {
        return Bytes.utf8("key" + String.format("%016d", keyNum));
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
