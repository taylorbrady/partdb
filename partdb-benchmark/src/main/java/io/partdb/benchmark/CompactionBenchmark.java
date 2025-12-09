package io.partdb.benchmark;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.compaction.CompactionConfig;
import io.partdb.storage.compaction.Compactor;
import io.partdb.storage.manifest.SSTableInfo;
import io.partdb.storage.sstable.SSTable;
import io.partdb.storage.sstable.SSTableConfig;
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
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class CompactionBenchmark {

    @Param({"2", "4", "8"})
    private int numSSTables;

    @Param({"10000", "50000"})
    private int entriesPerSSTable;

    @Param({"100", "1024"})
    private int valueSize;

    private Path tempDir;
    private List<SSTable> sourceTables;
    private List<Path> sourceTablePaths;
    private Compactor compactor;
    private AtomicLong idCounter;
    private Timestamp oldestSnapshot;
    private long totalInputBytes;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        tempDir = Files.createTempDirectory("compaction-bench");
        idCounter = new AtomicLong(1000);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        sourceTables = new ArrayList<>();
        sourceTablePaths = new ArrayList<>();
        totalInputBytes = 0;

        byte[] valueTemplate = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(valueTemplate);
        ByteArray value = ByteArray.copyOf(valueTemplate);

        for (int t = 0; t < numSSTables; t++) {
            long tableId = idCounter.getAndIncrement();
            Path path = tempDir.resolve("source-" + tableId + ".sst");
            sourceTablePaths.add(path);

            try (SSTable.Writer writer = SSTable.Writer.create(path, SSTableConfig.defaults())) {
                for (int i = 0; i < entriesPerSSTable; i++) {
                    int keyNum = i * numSSTables + t;
                    ByteArray key = ByteArray.copyOf(String.format("key%016d", keyNum).getBytes());
                    Timestamp ts = Timestamp.of(System.currentTimeMillis(), t * entriesPerSSTable + i);
                    writer.add(new Entry.Put(key, ts, value));
                }
            }

            totalInputBytes += Files.size(path);
            sourceTables.add(SSTable.open(path));
        }

        compactor = new Compactor(
            idCounter::getAndIncrement,
            id -> tempDir.resolve("output-" + id + ".sst"),
            SSTableConfig.defaults(),
            CompactionConfig.defaults()
        );

        oldestSnapshot = Timestamp.of(0, 0);
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws IOException {
        for (SSTable table : sourceTables) {
            table.close();
        }

        try (var paths = Files.list(tempDir)) {
            paths.filter(p -> p.toString().endsWith(".sst"))
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException _) {}
                });
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws IOException {
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
    public void compactMerge(Blackhole bh) {
        List<SSTableInfo> outputs = compactor.execute(
            sourceTables,
            1,
            false,
            oldestSnapshot
        );
        bh.consume(outputs);
    }

    @Benchmark
    public void compactWithTombstoneGC(Blackhole bh) {
        List<SSTableInfo> outputs = compactor.execute(
            sourceTables,
            1,
            true,
            oldestSnapshot
        );
        bh.consume(outputs);
    }
}
