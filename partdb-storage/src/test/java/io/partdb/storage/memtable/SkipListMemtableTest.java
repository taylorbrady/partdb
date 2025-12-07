package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.storage.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SkipListMemtableTest {

    private Memtable memtable;

    @BeforeEach
    void setUp() {
        memtable = new SkipListMemtable(MemtableConfig.create());
    }

    private static ByteArray key(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray value(int i) {
        return ByteArray.of((byte) i);
    }

    private static Entry.Data entry(int key, int value) {
        return new Entry.Data(key(key), value(value));
    }

    private static ByteArray largeValue(int size) {
        return ByteArray.wrap(new byte[size]);
    }

    private List<Entry> collectEntries(Iterator<Entry> iterator) {
        List<Entry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }

    @Nested
    class BasicOperations {

        @Test
        void putAndGet() {
            memtable.put(entry(1, 2));

            Optional<Entry> result = memtable.get(key(1));

            assertThat(result).isPresent();
            assertThat(result.get().key()).isEqualTo(key(1));
            assertThat(result.get()).isInstanceOf(Entry.Data.class);
            assertThat(((Entry.Data) result.get()).value()).isEqualTo(value(2));
        }

        @Test
        void getNonExistentKeyReturnsEmpty() {
            Optional<Entry> result = memtable.get(key(99));
            assertThat(result).isEmpty();
        }

        @Test
        void putOverwritesExistingEntry() {
            memtable.put(entry(1, 10));
            memtable.put(entry(1, 20));

            Optional<Entry> result = memtable.get(key(1));

            assertThat(result).isPresent();
            assertThat(((Entry.Data) result.get()).value()).isEqualTo(value(20));
        }

        @Test
        void putTombstone() {
            memtable.put(new Entry.Tombstone(key(5)));

            Optional<Entry> result = memtable.get(key(5));

            assertThat(result).isPresent();
            assertThat(result.get()).isInstanceOf(Entry.Tombstone.class);
        }

        @Test
        void deleteOverwritesExistingEntry() {
            memtable.put(entry(1, 10));
            memtable.put(new Entry.Tombstone(key(1)));

            Optional<Entry> result = memtable.get(key(1));

            assertThat(result).isPresent();
            assertThat(result.get()).isInstanceOf(Entry.Tombstone.class);
        }

        @Test
        void clear() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));

            memtable.clear();

            assertThat(memtable.entryCount()).isEqualTo(0);
            assertThat(memtable.sizeInBytes()).isEqualTo(0);
            assertThat(memtable.get(key(1))).isEmpty();
        }
    }

    @Nested
    class SizeTracking {

        @Test
        void initiallyZero() {
            assertThat(memtable.sizeInBytes()).isEqualTo(0);
        }

        @Test
        void increasesWithPut() {
            long initialSize = memtable.sizeInBytes();
            memtable.put(new Entry.Data(key(1), value(2)));

            assertThat(memtable.sizeInBytes()).isGreaterThan(initialSize);
        }

        @Test
        void updatesOnOverwrite() {
            memtable.put(new Entry.Data(key(1), value(1)));
            long sizeAfterSmall = memtable.sizeInBytes();

            memtable.put(new Entry.Data(key(1), largeValue(1000)));
            long sizeAfterLarge = memtable.sizeInBytes();

            assertThat(sizeAfterLarge).isGreaterThan(sizeAfterSmall);
        }
    }

    @Nested
    class EntryCount {

        @Test
        void initiallyZero() {
            assertThat(memtable.entryCount()).isEqualTo(0);
        }

        @Test
        void increasesWithNewEntries() {
            memtable.put(entry(1, 10));
            assertThat(memtable.entryCount()).isEqualTo(1);

            memtable.put(entry(2, 20));
            assertThat(memtable.entryCount()).isEqualTo(2);
        }

        @Test
        void unchangedOnOverwrite() {
            memtable.put(entry(1, 10));
            memtable.put(entry(1, 20));

            assertThat(memtable.entryCount()).isEqualTo(1);
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));

            Iterator<Entry> it = memtable.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).key()).isEqualTo(key(1));
            assertThat(entries.get(1).key()).isEqualTo(key(2));
            assertThat(entries.get(2).key()).isEqualTo(key(3));
        }

        @Test
        void withStartKeyOnly() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));

            Iterator<Entry> it = memtable.scan(key(2), null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(key(2));
            assertThat(entries.get(1).key()).isEqualTo(key(3));
        }

        @Test
        void withEndKeyOnly() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));

            Iterator<Entry> it = memtable.scan(null, key(3));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(key(1));
            assertThat(entries.get(1).key()).isEqualTo(key(2));
        }

        @Test
        void withBothBounds() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));
            memtable.put(entry(4, 40));

            Iterator<Entry> it = memtable.scan(key(2), key(4));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(key(2));
            assertThat(entries.get(1).key()).isEqualTo(key(3));
        }

        @Test
        void emptyRange() {
            memtable.put(entry(1, 10));

            Iterator<Entry> it = memtable.scan(key(5), key(10));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).isEmpty();
        }

        @Test
        void returnsSortedOrder() {
            memtable.put(entry(3, 30));
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));

            Iterator<Entry> it = memtable.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).key()).isEqualTo(key(1));
            assertThat(entries.get(1).key()).isEqualTo(key(2));
            assertThat(entries.get(2).key()).isEqualTo(key(3));
        }
    }

    @Nested
    class Concurrency {

        @Test
        void concurrentPuts() throws InterruptedException {
            int threadCount = 10;
            int entriesPerThread = 100;

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < entriesPerThread; j++) {
                            int keyValue = threadId * entriesPerThread + j;
                            memtable.put(new Entry.Data(key(keyValue & 0xFF), value(keyValue)));
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(10, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(memtable.entryCount()).isGreaterThan(0);
            assertThat(memtable.sizeInBytes()).isGreaterThan(0);
        }

        @Test
        void concurrentReadsAndWrites() throws InterruptedException {
            int writerCount = 5;
            int readerCount = 5;

            ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount);
            CountDownLatch latch = new CountDownLatch(writerCount + readerCount);

            for (int i = 0; i < writerCount; i++) {
                int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < 100; j++) {
                            memtable.put(new Entry.Data(key(threadId * 100 + j), value(j)));
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            for (int i = 0; i < readerCount; i++) {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < 100; j++) {
                            memtable.get(key(j));
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(10, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(memtable.entryCount()).isGreaterThan(0);
        }
    }
}
