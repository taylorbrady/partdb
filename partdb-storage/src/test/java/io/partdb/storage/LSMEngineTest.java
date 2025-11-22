package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.statemachine.Delete;
import io.partdb.common.statemachine.Put;
import io.partdb.storage.memtable.MemtableConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class LSMEngineTest {

    @TempDir
    Path tempDir;

    @Test
    void putAndGet() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);

            engine.apply(1, new Put(key, value, 0));

            Optional<ByteArray> result = engine.get(key);

            assertThat(result).isPresent();
            assertThat(result.get()).isEqualTo(value);
        }
    }

    @Test
    void getNonExistentKey() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            Optional<ByteArray> result = engine.get(ByteArray.of((byte) 99));
            assertThat(result).isEmpty();
        }
    }

    @Test
    void deleteKey() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);

            engine.apply(1, new Put(key, value, 0));
            engine.apply(2, new Delete(key));

            Optional<ByteArray> result = engine.get(key);
            assertThat(result).isEmpty();
        }
    }

    @Test
    void deleteNonExistentKey() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            engine.apply(1, new Delete(key));

            Optional<ByteArray> result = engine.get(key);
            assertThat(result).isEmpty();
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            engine.apply(1, new Put(key, ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(key, ByteArray.of((byte) 20), 0));

            Optional<ByteArray> result = engine.get(key);

            assertThat(result).isPresent();
            assertThat(result.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void expiredEntryReturnsEmpty() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);
            ByteArray value = ByteArray.of((byte) 10);
            long expiresAtMillis = System.currentTimeMillis() - 10000;

            engine.apply(1, new Put(key, value, expiresAtMillis));

            Optional<ByteArray> result = engine.get(key);
            assertThat(result).isEmpty();
        }
    }

    @Test
    void manualFlush() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            engine.apply(1, new Put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 0));

            engine.flush();

            Optional<ByteArray> result1 = engine.get(ByteArray.of((byte) 1));
            Optional<ByteArray> result2 = engine.get(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result2).isPresent();
        }
    }

    @Test
    void automaticFlushOnMemtableSize() {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        LSMEngineConfig config = new LSMEngineConfig(
            tempDir,
            memtableConfig,
            LSMEngineConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[200];

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            for (int i = 0; i < 10; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                engine.apply(i + 1, new Put(key, value, 0));
            }

            for (int i = 0; i < 10; i++) {
                Optional<ByteArray> result = engine.get(ByteArray.of((byte) i));
                assertThat(result).isPresent();
            }
        }
    }

    @Test
    void scanEntireRange() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            engine.apply(1, new Put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 0));
            engine.apply(3, new Put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 0));

            Iterator<Entry> it = engine.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanWithRange() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            engine.apply(1, new Put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 0));
            engine.apply(3, new Put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 0));
            engine.apply(4, new Put(ByteArray.of((byte) 4), ByteArray.of((byte) 40), 0));

            Iterator<Entry> it = engine.scan(ByteArray.of((byte) 2), ByteArray.of((byte) 4));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanFiltersTombstones() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            engine.apply(1, new Put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 0));
            engine.apply(3, new Delete(ByteArray.of((byte) 2)));
            engine.apply(4, new Put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 0));

            Iterator<Entry> it = engine.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanMergesFromMultipleSources() {
        MemtableConfig memtableConfig = new MemtableConfig(512);
        LSMEngineConfig config = new LSMEngineConfig(
            tempDir,
            memtableConfig,
            LSMEngineConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[100];

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                engine.apply(i + 1, new Put(key, value, 0));
            }

            Iterator<Entry> it = engine.scan(null, null);
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
        LSMEngineConfig config = new LSMEngineConfig(
            tempDir,
            memtableConfig,
            LSMEngineConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[100];

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            engine.apply(1, new Put(key, ByteArray.of((byte) 10), 0));

            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.wrap(largeValue), 0));

            engine.apply(3, new Put(key, ByteArray.of((byte) 20), 0));

            Iterator<Entry> it = engine.scan(null, null);
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
    void recoveryFromWAL() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            engine.apply(1, new Put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 0));
        }

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            Optional<ByteArray> result1 = engine.get(ByteArray.of((byte) 1));
            Optional<ByteArray> result2 = engine.get(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result1.get()).isEqualTo(ByteArray.of((byte) 10));

            assertThat(result2).isPresent();
            assertThat(result2.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void recoveryFromSSTables() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            engine.apply(1, new Put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 0));
            engine.apply(2, new Put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 0));
            engine.flush();
        }

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            Optional<ByteArray> result1 = engine.get(ByteArray.of((byte) 1));
            Optional<ByteArray> result2 = engine.get(ByteArray.of((byte) 2));

            assertThat(result1).isPresent();
            assertThat(result1.get()).isEqualTo(ByteArray.of((byte) 10));

            assertThat(result2).isPresent();
            assertThat(result2.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void readPathPriority() {
        LSMEngineConfig config = LSMEngineConfig.create(tempDir);

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            ByteArray key = ByteArray.of((byte) 1);

            engine.apply(1, new Put(key, ByteArray.of((byte) 10), 0));
            engine.flush();

            engine.apply(2, new Put(key, ByteArray.of((byte) 20), 0));

            Optional<ByteArray> result = engine.get(key);

            assertThat(result).isPresent();
            assertThat(result.get()).isEqualTo(ByteArray.of((byte) 20));
        }
    }

    @Test
    void multipleSSTables() {
        MemtableConfig memtableConfig = new MemtableConfig(512);
        LSMEngineConfig config = new LSMEngineConfig(
            tempDir,
            memtableConfig,
            LSMEngineConfig.create(tempDir).sstableConfig()
        );

        byte[] largeValue = new byte[100];

        try (LSMEngine engine = LSMEngine.open(tempDir, config)) {
            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                engine.apply(i + 1, new Put(key, value, 0));
            }

            engine.flush();

            for (int i = 0; i < 30; i++) {
                Optional<ByteArray> result = engine.get(ByteArray.of((byte) i));
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
