package io.partdb.benchmark;

import io.partdb.storage.StateStore;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedEntry;
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
    private StateStore store;
    private AtomicLong keyCounter;
    private AtomicLong revisionCounter;
    private byte[] valueTemplate;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("mixed-workload-bench");
        store = StateStore.open(tempDir, StorageConfig.defaults());
        keyCounter = new AtomicLong(0);
        revisionCounter = new AtomicLong(0);
        byte[] valueBytes = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueBytes);
        valueTemplate = valueBytes;

        for (int i = 0; i < initialKeyCount; i++) {
            byte[] key = formatKey(i);
            long revision = revisionCounter.incrementAndGet();
            store.put(key, valueTemplate, revision);
        }
        keyCounter.set(initialKeyCount);
        store.snapshot();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        store.close();
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

    private Optional<VersionedEntry> doRead() {
        long maxKey = keyCounter.get();
        long keyNum = ThreadLocalRandom.current().nextLong(maxKey);
        byte[] key = formatKey(keyNum);
        return store.get(key);
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
        long revision = revisionCounter.incrementAndGet();
        store.put(key, valueTemplate, revision);
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
