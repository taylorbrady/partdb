package io.partdb.benchmark;

import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedKeyValueStore;
import io.partdb.storage.VersionedValue;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class StoreReadBenchmark {

    private static final int VALUE_SIZE = 100;

    @Param({"10000", "100000"})
    private int keyCount;

    private Path tempDir;
    private VersionedKeyValueStore store;
    private Bytes[] existingKeys;
    private Bytes valueBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-read-bench");
        store = VersionedKeyValueStore.open(tempDir, StorageConfig.defaults());
        existingKeys = new Bytes[keyCount];

        byte[] bytes = new byte[VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(bytes);
        valueBytes = Bytes.copyOf(bytes);

        for (int i = 0; i < keyCount; i++) {
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
    public Optional<VersionedValue> pointGet() {
        int index = ThreadLocalRandom.current().nextInt(existingKeys.length);
        return store.get(existingKeys[index]);
    }

    @Benchmark
    public Optional<VersionedValue> pointGetMissing() {
        Bytes key = formatMissingKey(ThreadLocalRandom.current().nextInt());
        return store.get(key);
    }

    private static Bytes formatKey(long keyNum) {
        return Bytes.utf8("key" + String.format("%016d", keyNum));
    }

    private static Bytes formatMissingKey(int keyNum) {
        return Bytes.utf8("missing" + String.format("%012d", keyNum));
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
