package io.partdb.benchmark;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

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
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
public class MixedWorkloadBenchmark {

    @Param({"50", "95"})
    private int readPercent;

    @Param({"100000"})
    private int initialKeyCount;

    @Param({"100", "1024"})
    private int valueSize;

    private Path tempDir;
    private LSMTree tree;
    private AtomicLong keyCounter;
    private byte[] valueTemplate;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("mixed-workload-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        keyCounter = new AtomicLong(0);
        valueTemplate = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueTemplate);

        ByteArray value = ByteArray.copyOf(valueTemplate);
        for (int i = 0; i < initialKeyCount; i++) {
            ByteArray key = formatKey(i);
            Timestamp ts = Timestamp.of(System.currentTimeMillis(), i);
            tree.put(key, value, ts);
        }
        keyCounter.set(initialKeyCount);
        tree.flush();
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
    @Threads(4)
    public void mixed(Blackhole bh) {
        if (ThreadLocalRandom.current().nextInt(100) < readPercent) {
            doRead(bh);
        } else {
            doWrite();
        }
    }

    @Benchmark
    @Threads(1)
    public void mixedSingleThread(Blackhole bh) {
        if (ThreadLocalRandom.current().nextInt(100) < readPercent) {
            doRead(bh);
        } else {
            doWrite();
        }
    }

    private void doRead(Blackhole bh) {
        long maxKey = keyCounter.get();
        long keyNum = ThreadLocalRandom.current().nextLong(maxKey);
        ByteArray key = formatKey(keyNum);
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        try (var snapshot = tree.snapshot(ts)) {
            bh.consume(snapshot.get(key));
        }
    }

    private void doWrite() {
        long maxKey = keyCounter.get();
        long keyNum;
        if (ThreadLocalRandom.current().nextInt(100) < 50) {
            keyNum = keyCounter.getAndIncrement();
        } else {
            keyNum = ThreadLocalRandom.current().nextLong(maxKey);
        }
        ByteArray key = formatKey(keyNum);
        ByteArray value = ByteArray.copyOf(valueTemplate);
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        tree.put(key, value, ts);
    }

    private static ByteArray formatKey(long keyNum) {
        return ByteArray.copyOf(String.format("key%016d", keyNum).getBytes());
    }
}
