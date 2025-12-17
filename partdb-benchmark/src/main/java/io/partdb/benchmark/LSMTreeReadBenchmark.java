package io.partdb.benchmark;

import io.partdb.common.Entry;
import io.partdb.common.Slice;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
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
public class LSMTreeReadBenchmark {

    private static final int VALUE_SIZE = 100;

    @Param({"10000", "100000"})
    private int keyCount;

    private Path tempDir;
    private LSMTree tree;
    private Slice[] existingKeys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-read-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        existingKeys = new Slice[keyCount];

        byte[] valueBytes = new byte[VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(valueBytes);
        Slice value = Slice.of(valueBytes);

        for (int i = 0; i < keyCount; i++) {
            Slice key = formatKey(i);
            existingKeys[i] = key;
            tree.put(key, value, i);
        }

        tree.checkpoint();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tree.close();
        deleteDirectory(tempDir);
    }

    @Benchmark
    public Optional<Entry> pointGet() {
        int index = ThreadLocalRandom.current().nextInt(existingKeys.length);
        return tree.get(existingKeys[index]);
    }

    @Benchmark
    public Optional<Entry> pointGetMissing() {
        Slice key = formatMissingKey(ThreadLocalRandom.current().nextInt());
        return tree.get(key);
    }

    private static Slice formatKey(long keyNum) {
        return Slice.of(("key" + String.format("%016d", keyNum)).getBytes(StandardCharsets.UTF_8));
    }

    private static Slice formatMissingKey(int keyNum) {
        return Slice.of(("missing" + String.format("%012d", keyNum)).getBytes(StandardCharsets.UTF_8));
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
