package io.partdb.benchmark;

import io.partdb.common.Entry;
import io.partdb.common.Slice;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class LSMTreeScanBenchmark {

    private static final int KEY_COUNT = 100_000;
    private static final int VALUE_SIZE = 100;

    private Path tempDir;
    private LSMTree tree;
    private Slice[] existingKeys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-scan-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        existingKeys = new Slice[KEY_COUNT];

        byte[] valueBytes = new byte[VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes(valueBytes);
        Slice value = Slice.of(valueBytes);

        for (int i = 0; i < KEY_COUNT; i++) {
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
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long scanFull() {
        try (Stream<Entry> stream = tree.scan(null, null)) {
            return stream.count();
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(100)
    public void scanRange100(Blackhole bh) {
        int start = ThreadLocalRandom.current().nextInt(KEY_COUNT - 100);
        Slice startKey = existingKeys[start];
        Slice endKey = existingKeys[start + 100];
        try (Stream<Entry> stream = tree.scan(startKey, endKey)) {
            stream.forEach(bh::consume);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(1000)
    public void scanRange1000(Blackhole bh) {
        int start = ThreadLocalRandom.current().nextInt(KEY_COUNT - 1000);
        Slice startKey = existingKeys[start];
        Slice endKey = existingKeys[start + 1000];
        try (Stream<Entry> stream = tree.scan(startKey, endKey)) {
            stream.forEach(bh::consume);
        }
    }

    private static Slice formatKey(long keyNum) {
        return Slice.of(("key" + String.format("%016d", keyNum)).getBytes(StandardCharsets.UTF_8));
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
