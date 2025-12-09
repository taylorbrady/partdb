package io.partdb.benchmark;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.KeyValue;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class LSMTreeReadBenchmark {

    @Param({"1000", "10000", "100000"})
    private int keyCount;

    private Path tempDir;
    private LSMTree tree;
    private List<ByteArray> existingKeys;
    private Timestamp readTimestamp;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-read-bench");
        tree = LSMTree.open(tempDir, LSMConfig.defaults());
        existingKeys = new ArrayList<>(keyCount);

        byte[] value = new byte[100];
        ThreadLocalRandom.current().nextBytes(value);
        ByteArray valueArray = ByteArray.copyOf(value);

        for (int i = 0; i < keyCount; i++) {
            ByteArray key = ByteArray.copyOf(String.format("key%016d", i).getBytes());
            existingKeys.add(key);
            Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
            tree.put(key, valueArray, ts);
        }

        tree.flush();
        readTimestamp = Timestamp.of(System.currentTimeMillis(), 0);
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
    public void getExistingKey(Blackhole bh) {
        int index = ThreadLocalRandom.current().nextInt(existingKeys.size());
        ByteArray key = existingKeys.get(index);
        try (LSMTree.Snapshot snapshot = tree.snapshot(readTimestamp)) {
            bh.consume(snapshot.get(key));
        }
    }

    @Benchmark
    public void getMissingKey(Blackhole bh) {
        ByteArray key = ByteArray.copyOf(String.format("missing%016d", ThreadLocalRandom.current().nextInt()).getBytes());
        try (LSMTree.Snapshot snapshot = tree.snapshot(readTimestamp)) {
            bh.consume(snapshot.get(key));
        }
    }

    @Benchmark
    public void scanAll(Blackhole bh) {
        try (LSMTree.Snapshot snapshot = tree.snapshot(readTimestamp);
             Stream<KeyValue> stream = snapshot.scan(null, null)) {
            bh.consume(stream.count());
        }
    }

    @Benchmark
    public void scanRange100(Blackhole bh) {
        int start = ThreadLocalRandom.current().nextInt(Math.max(1, existingKeys.size() - 100));
        ByteArray startKey = existingKeys.get(start);
        ByteArray endKey = existingKeys.get(Math.min(start + 100, existingKeys.size() - 1));
        try (LSMTree.Snapshot snapshot = tree.snapshot(readTimestamp);
             Stream<KeyValue> stream = snapshot.scan(startKey, endKey)) {
            bh.consume(stream.count());
        }
    }

    @Benchmark
    public void scanRange1000(Blackhole bh) {
        int start = ThreadLocalRandom.current().nextInt(Math.max(1, existingKeys.size() - 1000));
        ByteArray startKey = existingKeys.get(start);
        ByteArray endKey = existingKeys.get(Math.min(start + 1000, existingKeys.size() - 1));
        try (LSMTree.Snapshot snapshot = tree.snapshot(readTimestamp);
             Stream<KeyValue> stream = snapshot.scan(startKey, endKey)) {
            bh.consume(stream.count());
        }
    }
}
