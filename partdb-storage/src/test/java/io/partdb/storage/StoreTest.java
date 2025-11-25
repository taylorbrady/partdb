package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.LeaseProvider;
import io.partdb.storage.memtable.MemtableConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class StoreTest {

    @TempDir
    Path tempDir;

    @Test
    void putAndGet() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);

            store.put(Entry.putWithLease(key, value, 1, 0));

            Optional<Entry> result = store.getEntry(key);

            assertThat(result).isPresent();
            assertThat(result.get().value()).isEqualTo(value);
        }
    }

    @Test
    void getNonExistentKey() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            Optional<Entry> result = store.getEntry(ByteArray.of((byte) 99));
            assertThat(result).isEmpty();
        }
    }

    @Test
    void deleteKey() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);

            store.put(Entry.putWithLease(key, value, 1, 0));
            store.put(Entry.delete(key, 2));

            Optional<Entry> result = store.getEntry(key);
            assertThat(result).isPresent();
            assertThat(result.get().tombstone()).isTrue();
        }
    }

    @Test
    void deleteNonExistentKey() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(Entry.delete(key, 1));

            Optional<Entry> result = store.getEntry(key);
            assertThat(result).isPresent();
            assertThat(result.get().tombstone()).isTrue();
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(Entry.putWithLease(key, ByteArray.of((byte) 10), 1, 0));
            store.put(Entry.putWithLease(key, ByteArray.of((byte) 20), 2, 0));

            Optional<Entry> result = store.getEntry(key);

            assertThat(result).isPresent();
            assertThat(result.get().value()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void manualFlush() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            store.put(Entry.putWithLease(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2, 0));

            store.flush();

            Optional<Entry> result1 = store.getEntry(ByteArray.of((byte) 1));
            Optional<Entry> result2 = store.getEntry(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result2).isPresent();
        }
    }

    @Test
    void automaticFlushOnMemtableSize() {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            StoreConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[200];

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 10; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                store.put(Entry.putWithLease(key, value, i + 1, 0));
            }

            for (int i = 0; i < 10; i++) {
                Optional<Entry> result = store.getEntry(ByteArray.of((byte) i));
                assertThat(result).isPresent();
            }
        }
    }

    @Test
    void scanEntireRange() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            store.put(Entry.putWithLease(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 3, 0));

            Iterator<Entry> it = store.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanWithRange() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            store.put(Entry.putWithLease(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 3, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 4), ByteArray.of((byte) 40), 4, 0));

            Iterator<Entry> it = store.scan(ByteArray.of((byte) 2), ByteArray.of((byte) 4));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanIncludesTombstones() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            store.put(Entry.putWithLease(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2, 0));
            store.put(Entry.delete(ByteArray.of((byte) 2), 3));
            store.put(Entry.putWithLease(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 4, 0));

            Iterator<Entry> it = store.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(1).tombstone()).isTrue();
            assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanMergesFromMultipleSources() {
        MemtableConfig memtableConfig = new MemtableConfig(512);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            StoreConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[100];

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                store.put(Entry.putWithLease(key, value, i + 1, 0));
            }

            Iterator<Entry> it = store.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(20);
            for (int i = 0; i < 20; i++) {
                assertThat(entries.get(i).key()).isEqualTo(ByteArray.of((byte) i));
            }
        }
    }

    @Test
    void scanUsesLatestValueForDuplicateKeys() {
        MemtableConfig memtableConfig = new MemtableConfig(256);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            StoreConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[100];

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(Entry.putWithLease(key, ByteArray.of((byte) 10), 1, 0));

            store.put(Entry.putWithLease(ByteArray.of((byte) 2), ByteArray.wrap(largeValue), 2, 0));

            store.put(Entry.putWithLease(key, ByteArray.of((byte) 20), 3, 0));

            Iterator<Entry> it = store.scan(null, null);
            List<Entry> entries = collectEntries(it);

            Optional<Entry> keyEntry = entries.stream()
                .filter(e -> e.key().equals(key))
                .findFirst();

            assertThat(keyEntry).isPresent();
            assertThat(keyEntry.get().value()).isEqualTo(ByteArray.of((byte) 20));
            assertThat(keyEntry.get().timestamp()).isEqualTo(3);
        }
    }

    @Test
    void recoveryFromSSTables() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            store.put(Entry.putWithLease(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 1, 0));
            store.put(Entry.putWithLease(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 2, 0));
            store.flush();
        }

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            Optional<Entry> result1 = store.getEntry(ByteArray.of((byte) 1));
            Optional<Entry> result2 = store.getEntry(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result1.get().value()).isEqualTo(ByteArray.of((byte) 10));

            assertThat(result2).isPresent();
            assertThat(result2.get().value()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void readPathPriority() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(Entry.putWithLease(key, ByteArray.of((byte) 10), 1, 0));
            store.flush();

            store.put(Entry.putWithLease(key, ByteArray.of((byte) 20), 2, 0));

            Optional<Entry> result = store.getEntry(key);

            assertThat(result).isPresent();
            assertThat(result.get().value()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void multipleSSTables() {
        MemtableConfig memtableConfig = new MemtableConfig(512);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            StoreConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[100];

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                store.put(Entry.putWithLease(key, value, i + 1, 0));
            }

            store.flush();

            for (int i = 0; i < 30; i++) {
                Optional<Entry> result = store.getEntry(ByteArray.of((byte) i));
                assertThat(result).isPresent();
            }
        }
    }

    private List<Entry> collectEntries(Iterator<Entry> iterator) {
        List<Entry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }
}
