package io.partdb.benchmark;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.ScanMode;
import io.partdb.storage.memtable.Memtable;
import io.partdb.storage.memtable.SkipListMemtable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class MemtableBenchmark {

    @Param({"1000", "10000", "100000"})
    private int keyCount;

    private Memtable emptyMemtable;
    private Memtable populatedMemtable;
    private List<ByteArray> existingKeys;
    private AtomicLong keyCounter;
    private byte[] valueTemplate;
    private Timestamp readTimestamp;

    @Setup(Level.Trial)
    public void setup() {
        emptyMemtable = new SkipListMemtable();
        populatedMemtable = new SkipListMemtable();
        existingKeys = new ArrayList<>(keyCount);
        keyCounter = new AtomicLong(0);
        valueTemplate = new byte[100];
        ThreadLocalRandom.current().nextBytes(valueTemplate);

        for (int i = 0; i < keyCount; i++) {
            ByteArray key = ByteArray.copyOf(String.format("key%016d", i).getBytes());
            ByteArray value = ByteArray.copyOf(valueTemplate);
            Timestamp ts = Timestamp.of(System.currentTimeMillis(), i);
            existingKeys.add(key);
            populatedMemtable.put(new Entry.Put(key, ts, value));
        }

        readTimestamp = Timestamp.of(System.currentTimeMillis(), keyCount);
    }

    @Setup(Level.Invocation)
    public void resetEmptyMemtable() {
        if (emptyMemtable.entryCount() > 100_000) {
            emptyMemtable.clear();
        }
    }

    @Benchmark
    public void put() {
        long seq = keyCounter.incrementAndGet();
        ByteArray key = ByteArray.copyOf(String.format("key%016d", seq).getBytes());
        ByteArray value = ByteArray.copyOf(valueTemplate);
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        emptyMemtable.put(new Entry.Put(key, ts, value));
    }

    @Benchmark
    public void getExisting(Blackhole bh) {
        int index = ThreadLocalRandom.current().nextInt(existingKeys.size());
        ByteArray key = existingKeys.get(index);
        bh.consume(populatedMemtable.get(key, readTimestamp));
    }

    @Benchmark
    public void getMissing(Blackhole bh) {
        ByteArray key = ByteArray.copyOf(String.format("missing%016d", ThreadLocalRandom.current().nextInt()).getBytes());
        bh.consume(populatedMemtable.get(key, readTimestamp));
    }

    @Benchmark
    public void scanAll(Blackhole bh) {
        Iterator<Entry> iterator = populatedMemtable.scan(
            new ScanMode.Snapshot(readTimestamp), null, null
        );
        int count = 0;
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
            count++;
        }
        bh.consume(count);
    }

    @Benchmark
    @Threads(4)
    public void putConcurrent() {
        long seq = keyCounter.incrementAndGet();
        ByteArray key = ByteArray.copyOf(String.format("key%016d", seq).getBytes());
        ByteArray value = ByteArray.copyOf(valueTemplate);
        Timestamp ts = Timestamp.of(System.currentTimeMillis(), 0);
        emptyMemtable.put(new Entry.Put(key, ts, value));
    }

    @Benchmark
    @Threads(4)
    public void getConcurrent(Blackhole bh) {
        int index = ThreadLocalRandom.current().nextInt(existingKeys.size());
        ByteArray key = existingKeys.get(index);
        bh.consume(populatedMemtable.get(key, readTimestamp));
    }
}
