package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.ScanMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SkipListMemtableTest {

    private Memtable memtable;

    @BeforeEach
    void setUp() {
        memtable = new SkipListMemtable();
    }

    private static ByteArray key(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray value(int i) {
        return ByteArray.of((byte) i);
    }

    private static Entry.Put entry(int key, int value) {
        return new Entry.Put(key(key), Timestamp.of(key, 0), value(value));
    }

    private static ByteArray largeValue(int size) {
        return ByteArray.copyOf(new byte[size]);
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

            Optional<Entry> result = memtable.get(key(1), Timestamp.MAX);

            assertTrue(result.isPresent());
            assertEquals(key(1), result.get().key());
            assertInstanceOf(Entry.Put.class, result.get());
            assertEquals(value(2), ((Entry.Put) result.get()).value());
        }

        @Test
        void getNonExistentKeyReturnsEmpty() {
            Optional<Entry> result = memtable.get(key(99), Timestamp.MAX);
            assertTrue(result.isEmpty());
        }

        @Test
        void putOverwritesExistingEntry() {
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 0), value(10)));
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 1), value(20)));

            Optional<Entry> result = memtable.get(key(1), Timestamp.MAX);

            assertTrue(result.isPresent());
            assertEquals(value(20), ((Entry.Put) result.get()).value());
        }

        @Test
        void putTombstone() {
            memtable.put(new Entry.Tombstone(key(5), Timestamp.of(5, 0)));

            Optional<Entry> result = memtable.get(key(5), Timestamp.MAX);

            assertTrue(result.isPresent());
            assertInstanceOf(Entry.Tombstone.class, result.get());
        }

        @Test
        void deleteOverwritesExistingEntry() {
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 0), value(10)));
            memtable.put(new Entry.Tombstone(key(1), Timestamp.of(1, 1)));

            Optional<Entry> result = memtable.get(key(1), Timestamp.MAX);

            assertTrue(result.isPresent());
            assertInstanceOf(Entry.Tombstone.class, result.get());
        }

        @Test
        void clear() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));

            memtable.clear();

            assertEquals(0, memtable.entryCount());
            assertEquals(0, memtable.sizeInBytes());
            assertTrue(memtable.get(key(1), Timestamp.MAX).isEmpty());
        }
    }

    @Nested
    class SizeTracking {

        @Test
        void initiallyZero() {
            assertEquals(0, memtable.sizeInBytes());
        }

        @Test
        void increasesWithPut() {
            long initialSize = memtable.sizeInBytes();
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 0), value(2)));

            assertTrue(memtable.sizeInBytes() > initialSize);
        }

        @Test
        void updatesOnOverwrite() {
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 0), value(1)));
            long sizeAfterSmall = memtable.sizeInBytes();

            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 1), largeValue(1000)));
            long sizeAfterLarge = memtable.sizeInBytes();

            assertTrue(sizeAfterLarge > sizeAfterSmall);
        }
    }

    @Nested
    class EntryCount {

        @Test
        void initiallyZero() {
            assertEquals(0, memtable.entryCount());
        }

        @Test
        void increasesWithNewEntries() {
            memtable.put(entry(1, 10));
            assertEquals(1, memtable.entryCount());

            memtable.put(entry(2, 20));
            assertEquals(2, memtable.entryCount());
        }

        @Test
        void increasesOnNewVersion() {
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 0), value(10)));
            memtable.put(new Entry.Put(key(1), Timestamp.of(1, 1), value(20)));

            assertEquals(2, memtable.entryCount());
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));

            Iterator<Entry> it = memtable.scan(new ScanMode.Snapshot(Timestamp.MAX), null, null);
            List<Entry> entries = collectEntries(it);

            assertEquals(3, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(2), entries.get(1).key());
            assertEquals(key(3), entries.get(2).key());
        }

        @Test
        void withStartKeyOnly() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));

            Iterator<Entry> it = memtable.scan(new ScanMode.Snapshot(Timestamp.MAX), key(2), null);
            List<Entry> entries = collectEntries(it);

            assertEquals(2, entries.size());
            assertEquals(key(2), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }

        @Test
        void withEndKeyOnly() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));

            Iterator<Entry> it = memtable.scan(new ScanMode.Snapshot(Timestamp.MAX), null, key(3));
            List<Entry> entries = collectEntries(it);

            assertEquals(2, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(2), entries.get(1).key());
        }

        @Test
        void withBothBounds() {
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));
            memtable.put(entry(3, 30));
            memtable.put(entry(4, 40));

            Iterator<Entry> it = memtable.scan(new ScanMode.Snapshot(Timestamp.MAX), key(2), key(4));
            List<Entry> entries = collectEntries(it);

            assertEquals(2, entries.size());
            assertEquals(key(2), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }

        @Test
        void emptyRange() {
            memtable.put(entry(1, 10));

            Iterator<Entry> it = memtable.scan(new ScanMode.Snapshot(Timestamp.MAX), key(5), key(10));
            List<Entry> entries = collectEntries(it);

            assertTrue(entries.isEmpty());
        }

        @Test
        void returnsSortedOrder() {
            memtable.put(entry(3, 30));
            memtable.put(entry(1, 10));
            memtable.put(entry(2, 20));

            Iterator<Entry> it = memtable.scan(new ScanMode.Snapshot(Timestamp.MAX), null, null);
            List<Entry> entries = collectEntries(it);

            assertEquals(3, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(2), entries.get(1).key());
            assertEquals(key(3), entries.get(2).key());
        }
    }

    @Nested
    class Concurrency {

        @Test
        void concurrentPuts() throws InterruptedException {
            int threadCount = 10;
            int entriesPerThread = 100;

            try (var executor = Executors.newFixedThreadPool(threadCount)) {
                for (int i = 0; i < threadCount; i++) {
                    int threadId = i;
                    executor.submit(() -> {
                        for (int j = 0; j < entriesPerThread; j++) {
                            int keyValue = threadId * entriesPerThread + j;
                            memtable.put(new Entry.Put(key(keyValue & 0xFF), Timestamp.of(keyValue, 0), value(keyValue)));
                        }
                    });
                }

                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Concurrent put tasks did not complete in time");
            }

            assertTrue(memtable.entryCount() > 0);
            assertTrue(memtable.sizeInBytes() > 0);
        }

        @Test
        void concurrentReadsAndWrites() throws InterruptedException {
            int writerCount = 5;
            int readerCount = 5;

            try (var executor = Executors.newFixedThreadPool(writerCount + readerCount)) {
                for (int i = 0; i < writerCount; i++) {
                    int threadId = i;
                    executor.submit(() -> {
                        for (int j = 0; j < 100; j++) {
                            memtable.put(new Entry.Put(key(threadId * 100 + j), Timestamp.of(j, 0), value(j)));
                        }
                    });
                }

                for (int i = 0; i < readerCount; i++) {
                    executor.submit(() -> {
                        for (int j = 0; j < 100; j++) {
                            memtable.get(key(j), Timestamp.MAX);
                        }
                    });
                }

                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Concurrent read/write tasks did not complete in time");
            }

            assertTrue(memtable.entryCount() > 0);
        }
    }
}
