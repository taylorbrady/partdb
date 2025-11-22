package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class SkipListMemtableTest {

    private Memtable memtable;

    @BeforeEach
    void setUp() {
        MemtableConfig config = MemtableConfig.create();
        memtable = new SkipListMemtable(config);
    }

    @Test
    void putAndGetSingleEntry() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        Entry entry = Entry.put(key, value, 1000L);

        memtable.put(entry);

        Optional<Entry> result = memtable.get(key);
        assertThat(result).isPresent();
        assertThat(result.get().key()).isEqualTo(key);
        assertThat(result.get().value()).isEqualTo(value);
        assertThat(result.get().tombstone()).isFalse();
    }

    @Test
    void getNonExistentKeyReturnsEmpty() {
        ByteArray key = ByteArray.of((byte) 99);

        Optional<Entry> result = memtable.get(key);

        assertThat(result).isEmpty();
    }

    @Test
    void putOverwritesExistingEntry() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value1 = ByteArray.of((byte) 10);
        ByteArray value2 = ByteArray.of((byte) 20);

        memtable.put(Entry.put(key, value1, 1000L));
        memtable.put(Entry.put(key, value2, 2000L));

        Optional<Entry> result = memtable.get(key);
        assertThat(result).isPresent();
        assertThat(result.get().value()).isEqualTo(value2);
        assertThat(result.get().timestamp()).isEqualTo(2000L);
    }

    @Test
    void putTombstoneEntry() {
        ByteArray key = ByteArray.of((byte) 5);
        Entry deleteEntry = Entry.delete(key, 3000L);

        memtable.put(deleteEntry);

        Optional<Entry> result = memtable.get(key);
        assertThat(result).isPresent();
        assertThat(result.get().tombstone()).isTrue();
        assertThat(result.get().value()).isNull();
    }

    @Test
    void deletingExistingKeyWritesTombstone() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 10);

        memtable.put(Entry.put(key, value, 1000L));
        memtable.put(Entry.delete(key, 2000L));

        Optional<Entry> result = memtable.get(key);
        assertThat(result).isPresent();
        assertThat(result.get().tombstone()).isTrue();
    }

    @Test
    void getExpiredEntryReturnsEmpty() throws InterruptedException {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        Entry entry = Entry.putWithTTL(key, value, System.currentTimeMillis(), 50L);

        memtable.put(entry);

        Thread.sleep(100);

        Optional<Entry> result = memtable.get(key);
        assertThat(result).isEmpty();
    }

    @Test
    void getNonExpiredEntryReturnsValue() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        Entry entry = Entry.putWithTTL(key, value, System.currentTimeMillis(), 10000L);

        memtable.put(entry);

        Optional<Entry> result = memtable.get(key);
        assertThat(result).isPresent();
        assertThat(result.get().value()).isEqualTo(value);
    }

    @Test
    void sizeInBytesInitiallyZero() {
        assertThat(memtable.sizeInBytes()).isEqualTo(0);
    }

    @Test
    void sizeInBytesIncreasesWithPut() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2, (byte) 3, (byte) 4);

        long initialSize = memtable.sizeInBytes();
        memtable.put(Entry.put(key, value, 1000L));

        assertThat(memtable.sizeInBytes()).isGreaterThan(initialSize);
    }

    @Test
    void sizeInBytesUpdatesOnOverwrite() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray smallValue = ByteArray.of((byte) 1);
        byte[] largeValue = new byte[1000];

        memtable.put(Entry.put(key, smallValue, 1000L));
        long sizeAfterSmall = memtable.sizeInBytes();

        memtable.put(Entry.put(key, ByteArray.wrap(largeValue), 2000L));
        long sizeAfterLarge = memtable.sizeInBytes();

        assertThat(sizeAfterLarge).isGreaterThan(sizeAfterSmall);
    }

    @Test
    void entryCountInitiallyZero() {
        assertThat(memtable.entryCount()).isEqualTo(0);
    }

    @Test
    void entryCountIncreasesWithNewEntries() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1000L));
        assertThat(memtable.entryCount()).isEqualTo(1);

        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2000L));
        assertThat(memtable.entryCount()).isEqualTo(2);
    }

    @Test
    void entryCountUnchangedOnOverwrite() {
        ByteArray key = ByteArray.of((byte) 1);

        memtable.put(Entry.put(key, ByteArray.of((byte) 10), 1000L));
        memtable.put(Entry.put(key, ByteArray.of((byte) 20), 2000L));

        assertThat(memtable.entryCount()).isEqualTo(1);
    }

    @Test
    void clearRemovesAllEntries() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1000L));
        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2000L));

        memtable.clear();

        assertThat(memtable.entryCount()).isEqualTo(0);
        assertThat(memtable.sizeInBytes()).isEqualTo(0);
        assertThat(memtable.get(ByteArray.of((byte) 1))).isEmpty();
    }

    @Test
    void scanEntireRange() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
        memtable.put(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));

        Iterator<Entry> it = memtable.scan(null, null);
        List<Entry> entries = collectEntries(it);

        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
        assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
        assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
    }

    @Test
    void scanWithStartKeyOnly() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
        memtable.put(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));

        Iterator<Entry> it = memtable.scan(ByteArray.of((byte) 2), null);
        List<Entry> entries = collectEntries(it);

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
        assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
    }

    @Test
    void scanWithEndKeyOnly() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
        memtable.put(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));

        Iterator<Entry> it = memtable.scan(null, ByteArray.of((byte) 3));
        List<Entry> entries = collectEntries(it);

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
        assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
    }

    @Test
    void scanWithBothStartAndEnd() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
        memtable.put(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));
        memtable.put(Entry.put(ByteArray.of((byte) 4), ByteArray.of((byte) 40), 400L));

        Iterator<Entry> it = memtable.scan(ByteArray.of((byte) 2), ByteArray.of((byte) 4));
        List<Entry> entries = collectEntries(it);

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
        assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
    }

    @Test
    void scanEmptyRange() {
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));

        Iterator<Entry> it = memtable.scan(ByteArray.of((byte) 5), ByteArray.of((byte) 10));
        List<Entry> entries = collectEntries(it);

        assertThat(entries).isEmpty();
    }

    @Test
    void scanReturnsSortedOrder() {
        memtable.put(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));
        memtable.put(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        memtable.put(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));

        Iterator<Entry> it = memtable.scan(null, null);
        List<Entry> entries = collectEntries(it);

        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
        assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
        assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
    }

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
                        ByteArray key = ByteArray.of((byte) (keyValue & 0xFF));
                        ByteArray value = ByteArray.of((byte) keyValue);
                        memtable.put(Entry.put(key, value, System.currentTimeMillis()));
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
                        ByteArray key = ByteArray.of((byte) (threadId * 100 + j));
                        ByteArray value = ByteArray.of((byte) j);
                        memtable.put(Entry.put(key, value, System.currentTimeMillis()));
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
                        ByteArray key = ByteArray.of((byte) j);
                        memtable.get(key);
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

    private List<Entry> collectEntries(Iterator<Entry> iterator) {
        List<Entry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }
}
