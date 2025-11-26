package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.KeyValue;
import io.partdb.storage.Store;
import io.partdb.storage.StoreConfig;
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

class CompactionTest {

    @TempDir
    Path tempDir;

    @Test
    void testL0CompactionTriggersWhenThresholdReached() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(key, value);
            }

            store.flush();

            Thread.sleep(500);

            ManifestData manifest = store.getManifest();
            List<SSTableMetadata> l0Files = manifest.level(0);
            List<SSTableMetadata> l1Files = manifest.level(1);

            assertThat(l0Files.size()).isLessThan(4);
            assertThat(l1Files.size()).isGreaterThan(0);
        }
    }

    @Test
    void testCompactionMergesOverlappingKeys() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            for (int version = 0; version < 5; version++) {
                for (int i = 0; i < 20; i++) {
                    ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                    ByteArray value = ByteArray.wrap(("v" + version + "-" + i).getBytes());
                    store.put(key, value);
                }
                store.flush();
            }

            Thread.sleep(1000);

            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                Optional<ByteArray> result = store.get(key);
                assertThat(result).isPresent();
                assertThat(new String(result.get().toByteArray())).startsWith("v4");
            }
        }
    }

    @Test
    void testTombstonesResultInEmptyGet() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(key, value);
            }
            store.flush();

            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                store.delete(key);
            }
            store.flush();

            Thread.sleep(500);

            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                Optional<ByteArray> result = store.get(key);
                assertThat(result).isEmpty();
            }
        }
    }

    @Test
    void testLevelSizeRespected() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(2048);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            byte[] largeValue = new byte[100];
            for (int batch = 0; batch < 20; batch++) {
                for (int i = 0; i < 100; i++) {
                    ByteArray key = ByteArray.wrap(String.format("key-%05d", batch * 100 + i).getBytes());
                    ByteArray value = ByteArray.wrap(largeValue);
                    store.put(key, value);
                }
                store.flush();
            }

            Thread.sleep(2000);

            ManifestData manifest = store.getManifest();
            LeveledCompactionConfig compactionConfig = LeveledCompactionConfig.create();

            for (int level = 1; level < manifest.maxLevel(); level++) {
                long levelSize = manifest.levelSize(level);
                long maxSize = compactionConfig.maxBytesForLevel(level);

                assertThat(levelSize).isLessThanOrEqualTo(maxSize * 2);
            }
        }
    }

    @Test
    void testCompactionPreservesNewestVersions() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            List<String> expectedValues = new ArrayList<>();

            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                String value = "version-" + i;
                expectedValues.add(value);
                store.put(key, ByteArray.wrap(value.getBytes()));

                if (i % 10 == 9) {
                    store.flush();
                }
            }

            Thread.sleep(1000);

            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                Optional<ByteArray> result = store.get(key);
                assertThat(result).isPresent();
                assertThat(new String(result.get().toByteArray())).isEqualTo(expectedValues.get(i));
            }
        }
    }

    @Test
    void testManifestConsistencyAfterCompaction() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 80; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(key, value);
            }
            store.flush();

            Thread.sleep(500);

            ManifestData manifest = store.getManifest();

            long totalEntries = 0;
            for (SSTableMetadata meta : manifest.sstables()) {
                totalEntries += meta.entryCount();
                assertThat(meta.id()).isGreaterThan(0);
                assertThat(meta.level()).isGreaterThanOrEqualTo(0);
                assertThat(meta.fileSizeBytes()).isGreaterThan(0);
                assertThat(meta.smallestKey()).isNotNull();
                assertThat(meta.largestKey()).isNotNull();
            }

            assertThat(totalEntries).isGreaterThanOrEqualTo(80);
        }
    }

    @Test
    void testReopenAfterCompaction() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        List<ByteArray> keys = new ArrayList<>();
        List<ByteArray> values = new ArrayList<>();

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 60; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                keys.add(key);
                values.add(value);
                store.put(key, value);
            }
            store.flush();
            Thread.sleep(500);
        }

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < keys.size(); i++) {
                Optional<ByteArray> result = store.get(keys.get(i));
                assertThat(result).isPresent();
                assertThat(result.get()).isEqualTo(values.get(i));
            }
        }
    }

    @Test
    void testScanAfterCompaction() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            memtableConfig,
            SSTableConfig.create(),
            Duration.ofHours(24)
        );

        try (Store store = Store.open(tempDir, config)) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(key, value);
            }
            store.flush();

            Thread.sleep(500);

            ByteArray startKey = ByteArray.wrap("key-020".getBytes());
            ByteArray endKey = ByteArray.wrap("key-030".getBytes());

            try (CloseableIterator<KeyValue> iterator = store.scan(startKey, endKey)) {
                int count = 0;
                while (iterator.hasNext()) {
                    KeyValue kv = iterator.next();
                    count++;
                    assertThat(kv.key().compareTo(startKey)).isGreaterThanOrEqualTo(0);
                    assertThat(kv.key().compareTo(endKey)).isLessThan(0);
                }

                assertThat(count).isEqualTo(10);
            }
        }
    }

    @Test
    void testEmptyManifestLoad() {
        StoreConfig config = StoreConfig.create();

        try (Store store = Store.open(tempDir, config)) {
            ManifestData manifest = store.getManifest();
            assertThat(manifest).isNotNull();
            assertThat(manifest.sstables()).isEmpty();
        }
    }
}
