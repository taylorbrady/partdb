package io.partdb.benchmark;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import io.partdb.storage.WriteBatch;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class LSMTreeWriteBenchmark {

    @Param({"100", "1024", "4096"})
    private int valueSize;

    private Path tempDir;
    private LSMTree tree;
    private AtomicLong keyCounter;
    private byte[] valueTemplate;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-write-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        keyCounter = new AtomicLong(0);
        valueTemplate = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueTemplate);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tree.close();
        try (var paths = Files.walk(tempDir)) {
            paths.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException _) {}
                });
        }
    }

    @Benchmark
    public void putSequential() {
        long seq = keyCounter.incrementAndGet();
        ByteArray key = ByteArray.copyOf(String.format("key%016d", seq).getBytes());
        ByteArray value = ByteArray.copyOf(valueTemplate);
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        tree.put(key, value, ts);
    }

    @Benchmark
    public void putRandom() {
        byte[] keyBytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(keyBytes);
        ByteArray key = ByteArray.copyOf(keyBytes);
        ByteArray value = ByteArray.copyOf(valueTemplate);
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        tree.put(key, value, ts);
    }

    @Benchmark
    @OperationsPerInvocation(10)
    public void writeBatch10() {
        WriteBatch.Builder builder = WriteBatch.builder();
        long base = keyCounter.addAndGet(10);
        for (int i = 0; i < 10; i++) {
            ByteArray key = ByteArray.copyOf(String.format("key%016d", base + i).getBytes());
            ByteArray value = ByteArray.copyOf(valueTemplate);
            builder.put(key, value);
        }
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        tree.write(builder.build(), ts);
    }

    @Benchmark
    @OperationsPerInvocation(100)
    public void writeBatch100() {
        WriteBatch.Builder builder = WriteBatch.builder();
        long base = keyCounter.addAndGet(100);
        for (int i = 0; i < 100; i++) {
            ByteArray key = ByteArray.copyOf(String.format("key%016d", base + i).getBytes());
            ByteArray value = ByteArray.copyOf(valueTemplate);
            builder.put(key, value);
        }
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        tree.write(builder.build(), ts);
    }
}
