package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.KeyValue;
import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class StoreTest {

    @TempDir
    Path tempDir;

    @Test
    void putAndGet() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);

            store.put(key, value);

            Optional<ByteArray> result = store.get(key);

            assertThat(result).isPresent();
            assertThat(result.get()).isEqualTo(value);
        }
    }

    @Test
    void getNonExistentKey() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            Optional<ByteArray> result = store.get(ByteArray.of((byte) 99));
            assertThat(result).isEmpty();
        }
    }

    @Test
    void deleteKey() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);

            store.put(key, value);
            store.delete(key);

            Optional<ByteArray> result = store.get(key);
            assertThat(result).isEmpty();
        }
    }

    @Test
    void deleteNonExistentKey() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            store.delete(key);

            Optional<ByteArray> result = store.get(key);
            assertThat(result).isEmpty();
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(key, ByteArray.of((byte) 10));
            store.put(key, ByteArray.of((byte) 20));

            Optional<ByteArray> result = store.get(key);

            assertThat(result).isPresent();
            assertThat(result.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void manualFlush() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            store.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10));
            store.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20));

            store.flush();

            Optional<ByteArray> result1 = store.get(ByteArray.of((byte) 1));
            Optional<ByteArray> result2 = store.get(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result2).isPresent();
        }
    }

    @Test
    void automaticFlushOnMemtableSize() {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        byte[] largeValue = new byte[200];

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 10; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                store.put(key, value);
            }

            for (int i = 0; i < 10; i++) {
                Optional<ByteArray> result = store.get(ByteArray.of((byte) i));
                assertThat(result).isPresent();
            }
        }
    }

    @Test
    void scanEntireRange() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            store.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10));
            store.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20));
            store.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30));

            try (CloseableIterator<KeyValue> it = store.scan(null, null)) {
                List<KeyValue> entries = collectEntries(it);

                assertThat(entries).hasSize(3);
                assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
                assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
                assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
            }
        }
    }

    @Test
    void scanWithRange() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            store.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10));
            store.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20));
            store.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30));
            store.put(ByteArray.of((byte) 4), ByteArray.of((byte) 40));

            try (CloseableIterator<KeyValue> it = store.scan(ByteArray.of((byte) 2), ByteArray.of((byte) 4))) {
                List<KeyValue> entries = collectEntries(it);

                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
                assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
            }
        }
    }

    @Test
    void scanExcludesDeletedKeys() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            store.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10));
            store.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20));
            store.delete(ByteArray.of((byte) 2));
            store.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30));

            try (CloseableIterator<KeyValue> it = store.scan(null, null)) {
                List<KeyValue> entries = collectEntries(it);

                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
                assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
            }
        }
    }

    @Test
    void scanMergesFromMultipleSources() {
        MemtableConfig memtableConfig = new MemtableConfig(512);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        byte[] largeValue = new byte[100];

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                store.put(key, value);
            }

            try (CloseableIterator<KeyValue> it = store.scan(null, null)) {
                List<KeyValue> entries = collectEntries(it);

                assertThat(entries).hasSize(20);
                for (int i = 0; i < 20; i++) {
                    assertThat(entries.get(i).key()).isEqualTo(ByteArray.of((byte) i));
                }
            }
        }
    }

    @Test
    void scanUsesLatestValueForDuplicateKeys() {
        MemtableConfig memtableConfig = new MemtableConfig(256);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        byte[] largeValue = new byte[100];

        try (Store store = Store.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(key, ByteArray.of((byte) 10));

            store.put(ByteArray.of((byte) 2), ByteArray.wrap(largeValue));

            store.put(key, ByteArray.of((byte) 20));

            try (CloseableIterator<KeyValue> it = store.scan(null, null)) {
                List<KeyValue> entries = collectEntries(it);

                Optional<KeyValue> keyEntry = entries.stream()
                    .filter(e -> e.key().equals(key))
                    .findFirst();

                assertThat(keyEntry).isPresent();
                assertThat(keyEntry.get().value()).isEqualTo(ByteArray.of((byte) 20));
            }
        }
    }

    @Test
    void recoveryFromSSTables() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            store.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10));
            store.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20));
            store.flush();
        }

        try (Store store = Store.open(tempDir, config)) {
            Optional<ByteArray> result1 = store.get(ByteArray.of((byte) 1));
            Optional<ByteArray> result2 = store.get(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result1.get()).isEqualTo(ByteArray.of((byte) 10));

            assertThat(result2).isPresent();
            assertThat(result2.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void readPathPriority() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            store.put(key, ByteArray.of((byte) 10));
            store.flush();

            store.put(key, ByteArray.of((byte) 20));

            Optional<ByteArray> result = store.get(key);

            assertThat(result).isPresent();
            assertThat(result.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void multipleSSTables() {
        MemtableConfig memtableConfig = new MemtableConfig(512);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        byte[] largeValue = new byte[100];

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                store.put(key, value);
            }

            store.flush();

            for (int i = 0; i < 30; i++) {
                Optional<ByteArray> result = store.get(ByteArray.of((byte) i));
                assertThat(result).isPresent();
            }
        }
    }

    private List<KeyValue> collectEntries(CloseableIterator<KeyValue> iterator) {
        List<KeyValue> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }
}
