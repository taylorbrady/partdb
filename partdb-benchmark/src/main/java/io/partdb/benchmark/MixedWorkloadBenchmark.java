package io.partdb.benchmark;

import io.partdb.common.Timestamp;
import io.partdb.storage.KeyValue;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
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
    private LSMTree.Snapshot readSnapshot;
    private AtomicLong keyCounter;
    private AtomicLong timestampCounter;
    private byte[] valueTemplate;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("mixed-workload-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        keyCounter = new AtomicLong(0);
        timestampCounter = new AtomicLong(0);
        valueTemplate = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueTemplate);

        for (int i = 0; i < initialKeyCount; i++) {
            byte[] key = formatKey(i);
            Timestamp ts = Timestamp.of(timestampCounter.incrementAndGet(), 0);
            tree.put(key, valueTemplate, ts);
        }
        keyCounter.set(initialKeyCount);
        tree.flush();

        Timestamp readTs = Timestamp.of(timestampCounter.get(), 0);
        readSnapshot = tree.snapshot(readTs);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        readSnapshot.close();
        tree.close();
        deleteDirectory(tempDir);
    }

    @Benchmark
    @Threads(4)
    public void mixedMultiThreaded(Blackhole bh) {
        if (ThreadLocalRandom.current().nextInt(100) < readPercent) {
            bh.consume(doRead());
        } else {
            doWrite();
        }
    }

    @Benchmark
    @Threads(1)
    public void mixedSingleThreaded(Blackhole bh) {
        if (ThreadLocalRandom.current().nextInt(100) < readPercent) {
            bh.consume(doRead());
        } else {
            doWrite();
        }
    }

    private Optional<KeyValue> doRead() {
        long maxKey = keyCounter.get();
        long keyNum = ThreadLocalRandom.current().nextLong(maxKey);
        byte[] key = formatKey(keyNum);
        return readSnapshot.get(key);
    }

    private void doWrite() {
        long maxKey = keyCounter.get();
        long keyNum;
        if (ThreadLocalRandom.current().nextInt(100) < 50) {
            keyNum = keyCounter.getAndIncrement();
        } else {
            keyNum = ThreadLocalRandom.current().nextLong(maxKey);
        }
        byte[] key = formatKey(keyNum);
        Timestamp ts = Timestamp.of(timestampCounter.incrementAndGet(), 0);
        tree.put(key, valueTemplate, ts);
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
