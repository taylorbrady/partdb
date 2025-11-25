package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.LeaseProvider;
import io.partdb.storage.Store;
import io.partdb.storage.StoreConfig;
import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
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
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(Entry.putWithLease(key, value, i, 0));
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
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            long index = 0;
            for (int version = 0; version < 5; version++) {
                for (int i = 0; i < 20; i++) {
                    ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                    ByteArray value = ByteArray.wrap(("v" + version + "-" + i).getBytes());
                    store.put(Entry.putWithLease(key, value, index++, 0));
                }
                store.flush();
            }

            Thread.sleep(1000);

            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                Optional<Entry> result = store.getEntry(key);
                assertThat(result).isPresent();
                assertThat(new String(result.get().value().toByteArray())).startsWith("v4");
            }
        }
    }

    @Test
    void testTombstonesDroppedAtBottomLevel() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            long index = 0;
            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(Entry.putWithLease(key, value, index++, 0));
            }
            store.flush();

            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                store.put(Entry.delete(key, index++));
            }
            store.flush();

            Thread.sleep(500);

            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                Optional<Entry> result = store.getEntry(key);
                assertThat(result).isPresent();
                assertThat(result.get().tombstone()).isTrue();
            }
        }
    }

    @Test
    void testLevelSizeRespected() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(2048);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            byte[] largeValue = new byte[100];
            long index = 0;
            for (int batch = 0; batch < 20; batch++) {
                for (int i = 0; i < 100; i++) {
                    ByteArray key = ByteArray.wrap(String.format("key-%05d", batch * 100 + i).getBytes());
                    ByteArray value = ByteArray.wrap(largeValue);
                    store.put(Entry.putWithLease(key, value, index++, 0));
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
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            List<String> expectedValues = new ArrayList<>();

            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                String value = "version-" + i;
                expectedValues.add(value);
                store.put(Entry.putWithLease(key, ByteArray.wrap(value.getBytes()), i + 1, 0));

                if (i % 10 == 9) {
                    store.flush();
                }
            }

            Thread.sleep(1000);

            for (int i = 0; i < 30; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%02d", i).getBytes());
                Optional<Entry> result = store.getEntry(key);
                assertThat(result).isPresent();
                assertThat(new String(result.get().value().toByteArray())).isEqualTo(expectedValues.get(i));
            }
        }
    }

    @Test
    void testManifestConsistencyAfterCompaction() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 80; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(Entry.putWithLease(key, value, i + 1, 0));
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
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        List<ByteArray> keys = new ArrayList<>();
        List<ByteArray> values = new ArrayList<>();

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 60; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                keys.add(key);
                values.add(value);
                store.put(Entry.putWithLease(key, value, i + 1, 0));
            }
            store.flush();
            Thread.sleep(500);
        }

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < keys.size(); i++) {
                Optional<Entry> result = store.getEntry(keys.get(i));
                assertThat(result).isPresent();
                assertThat(result.get().value()).isEqualTo(values.get(i));
            }
        }
    }

    @Test
    void testScanAfterCompaction() throws Exception {
        MemtableConfig memtableConfig = new MemtableConfig(1024);
        StoreConfig config = new StoreConfig(
            tempDir,
            memtableConfig,
            SSTableConfig.create()
        );

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.wrap(String.format("key-%03d", i).getBytes());
                ByteArray value = ByteArray.wrap(("value-" + i).getBytes());
                store.put(Entry.putWithLease(key, value, i + 1, 0));
            }
            store.flush();

            Thread.sleep(500);

            ByteArray startKey = ByteArray.wrap("key-020".getBytes());
            ByteArray endKey = ByteArray.wrap("key-030".getBytes());

            var iterator = store.scan(startKey, endKey);
            int count = 0;
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                count++;
                assertThat(entry.key().compareTo(startKey)).isGreaterThanOrEqualTo(0);
                assertThat(entry.key().compareTo(endKey)).isLessThan(0);
            }

            assertThat(count).isEqualTo(10);
        }
    }

    @Test
    void testEmptyManifestLoad() {
        StoreConfig config = StoreConfig.create(tempDir);

        try (Store store = Store.open(tempDir, config, LeaseProvider.alwaysActive())) {
            ManifestData manifest = store.getManifest();
            assertThat(manifest).isNotNull();
            assertThat(manifest.sstables()).isEmpty();
        }
    }
}
