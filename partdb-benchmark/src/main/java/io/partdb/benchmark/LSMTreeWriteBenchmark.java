package io.partdb.benchmark;

import io.partdb.common.Slice;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class LSMTreeWriteBenchmark {

    @Param({"100", "1024", "4096"})
    private int valueSize;

    private Path tempDir;
    private LSMTree tree;
    private AtomicLong keyCounter;
    private AtomicLong revisionCounter;
    private Slice valueTemplate;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-write-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        keyCounter = new AtomicLong(0);
        revisionCounter = new AtomicLong(0);
        byte[] valueBytes = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueBytes);
        valueTemplate = Slice.of(valueBytes);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tree.close();
        deleteDirectory(tempDir);
    }

    @Benchmark
    public void putSequential() {
        long seq = keyCounter.incrementAndGet();
        Slice key = formatKey(seq);
        long revision = revisionCounter.incrementAndGet();
        tree.put(key, valueTemplate, revision);
    }

    @Benchmark
    public void putRandom() {
        byte[] keyBytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(keyBytes);
        Slice key = Slice.of(keyBytes);
        long revision = revisionCounter.incrementAndGet();
        tree.put(key, valueTemplate, revision);
    }

    @Benchmark
    @OperationsPerInvocation(10)
    public void putBatch10() {
        long base = keyCounter.addAndGet(10);
        long revision = revisionCounter.incrementAndGet();
        for (int i = 0; i < 10; i++) {
            Slice key = formatKey(base + i);
            tree.put(key, valueTemplate, revision + i);
        }
    }

    @Benchmark
    @OperationsPerInvocation(100)
    public void putBatch100() {
        long base = keyCounter.addAndGet(100);
        long revision = revisionCounter.incrementAndGet();
        for (int i = 0; i < 100; i++) {
            Slice key = formatKey(base + i);
            tree.put(key, valueTemplate, revision + i);
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
