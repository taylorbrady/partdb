package io.partdb.benchmark;

import io.partdb.storage.StateStore;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedEntry;
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
public class LsmEngineReadBenchmark {

    private static final int VALUE_SIZE = 100;

    @Param({"10000", "100000"})
    private int keyCount;

    private Path tempDir;
    private StateStore store;
    private byte[][] existingKeys;
    private byte[] valueBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-read-bench");
        store = StateStore.open(tempDir, StorageConfig.defaults());
        existingKeys = new byte[keyCount][];

        valueBytes = new byte[VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(valueBytes);

        for (int i = 0; i < keyCount; i++) {
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
    public Optional<VersionedEntry> pointGet() {
        int index = ThreadLocalRandom.current().nextInt(existingKeys.length);
        return store.get(existingKeys[index]);
    }

    @Benchmark
    public Optional<VersionedEntry> pointGetMissing() {
        byte[] key = formatMissingKey(ThreadLocalRandom.current().nextInt());
        return store.get(key);
    }

    private static byte[] formatKey(long keyNum) {
        return ("key" + String.format("%016d", keyNum)).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] formatMissingKey(int keyNum) {
        return ("missing" + String.format("%012d", keyNum)).getBytes(StandardCharsets.UTF_8);
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
