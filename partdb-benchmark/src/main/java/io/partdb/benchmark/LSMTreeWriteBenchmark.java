package io.partdb.benchmark;

import io.partdb.common.Timestamp;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import io.partdb.storage.WriteBatch;
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
    private AtomicLong timestampCounter;
    private byte[] valueTemplate;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-write-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        keyCounter = new AtomicLong(0);
        timestampCounter = new AtomicLong(0);
        valueTemplate = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueTemplate);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tree.close();
        deleteDirectory(tempDir);
    }

    @Benchmark
    public void putSequential() {
        long seq = keyCounter.incrementAndGet();
        byte[] key = formatKey(seq);
        Timestamp ts = Timestamp.of(timestampCounter.incrementAndGet(), 0);
        tree.put(key, valueTemplate, ts);
    }

    @Benchmark
    public void putRandom() {
        byte[] key = new byte[16];
        ThreadLocalRandom.current().nextBytes(key);
        Timestamp ts = Timestamp.of(timestampCounter.incrementAndGet(), 0);
        tree.put(key, valueTemplate, ts);
    }

    @Benchmark
    @OperationsPerInvocation(10)
    public void writeBatch10() {
        WriteBatch.Builder builder = WriteBatch.builder();
        long base = keyCounter.addAndGet(10);
        for (int i = 0; i < 10; i++) {
            byte[] key = formatKey(base + i);
            builder.put(key, valueTemplate);
        }
        Timestamp ts = Timestamp.of(timestampCounter.incrementAndGet(), 0);
        tree.write(builder.build(), ts);
    }

    @Benchmark
    @OperationsPerInvocation(100)
    public void writeBatch100() {
        WriteBatch.Builder builder = WriteBatch.builder();
        long base = keyCounter.addAndGet(100);
        for (int i = 0; i < 100; i++) {
            byte[] key = formatKey(base + i);
            builder.put(key, valueTemplate);
        }
        Timestamp ts = Timestamp.of(timestampCounter.incrementAndGet(), 0);
        tree.write(builder.build(), ts);
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
