package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsCompactionTest extends StorageEngineInternalTestSupport {

    @Test
    void l0TriggersAtThreshold() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }

            tree.flush();
            awaitCompaction(tree);

            SSTableManifest manifest = tree.manifest();
            List<SSTableMetadata> l0Files = manifest.level(0);
            List<SSTableMetadata> l1Files = manifest.level(1);

            assertTrue(l0Files.size() < 4);
            assertTrue(l1Files.size() > 0);
        }
    }

    @Test
    void mergesOverlappingKeys() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int version = 0; version < 5; version++) {
                for (int i = 0; i < 20; i++) {
                    put(tree, key(String.format("key-%02d", i)), value("v" + version + "-" + i), nextRevision());
                }
                tree.flush();
            }

            awaitCompaction(tree);

            for (int i = 0; i < 20; i++) {
                Optional<StoredEntry.Value> result = tree.get(key(String.format("key-%02d", i)));
                assertTrue(result.isPresent());
                assertTrue(new String(result.get().value().toByteArray(), StandardCharsets.UTF_8).startsWith("v4"));
            }
        }
    }

    @Test
    void tombstonesResultInEmptyGet() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            tree.flush();

            for (int i = 0; i < 50; i++) {
                delete(tree, key(String.format("key-%03d", i)), nextRevision());
            }
            tree.flush();

            awaitCompaction(tree);

            for (int i = 0; i < 50; i++) {
                assertTrue(tree.get(key(String.format("key-%03d", i))).isEmpty());
            }
        }
    }

    @Test
    void levelSizeRespected() {
        LsmConfig config = smallMemtableConfig(2048);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int batch = 0; batch < 20; batch++) {
                for (int i = 0; i < 100; i++) {
                    put(tree, key(String.format("key-%05d", batch * 100 + i)), largeValue(100), nextRevision());
                }
                tree.flush();
            }

            awaitCompaction(tree);

            SSTableManifest manifest = tree.manifest();

            for (int level = 1; level < manifest.maxLevel(); level++) {
                long levelSize = manifest.levelSize(level);
                long maxSize = config.maxBytesForLevel(level);

                assertTrue(levelSize <= maxSize * 2);
            }
        }
    }

    @Test
    void preservesNewestVersions() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            List<String> expectedValues = new ArrayList<>();

            for (int i = 0; i < 30; i++) {
                String val = "version-" + i;
                expectedValues.add(val);
                put(tree, key(String.format("key-%02d", i)), value(val), nextRevision());

                if (i % 10 == 9) {
                    tree.flush();
                }
            }

            awaitCompaction(tree);

            for (int i = 0; i < 30; i++) {
                Optional<StoredEntry.Value> result = tree.get(key(String.format("key-%02d", i)));
                assertTrue(result.isPresent());
                assertEquals(expectedValues.get(i), new String(result.get().value().toByteArray(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void manifestConsistency() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int i = 0; i < 80; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            tree.flush();

            awaitCompaction(tree);

            SSTableManifest manifest = tree.manifest();

            long totalEntries = 0;
            for (SSTableMetadata desc : manifest.sstables()) {
                totalEntries += desc.entryCount();
                assertTrue(desc.id() > 0);
                assertTrue(desc.level() >= 0);
                assertTrue(desc.fileSizeBytes() > 0);
                assertNotNull(desc.smallestKey());
                assertNotNull(desc.largestKey());
            }

            assertTrue(totalEntries >= 80);
        }
    }

    @Test
    void reopenAfterCompaction() {
        LsmConfig config = smallMemtableConfig(1024);

        List<Slice> keys = new ArrayList<>();
        List<Slice> values = new ArrayList<>();

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int i = 0; i < 60; i++) {
                Slice k = key(String.format("key-%03d", i));
                Slice v = value("value-" + i);
                keys.add(k);
                values.add(v);
                put(tree, k, v, nextRevision());
            }
            tree.flush();
            awaitCompaction(tree);
        }

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int i = 0; i < keys.size(); i++) {
                Optional<StoredEntry.Value> result = tree.get(keys.get(i));
                assertTrue(result.isPresent());
                assertEquals(values.get(i), result.get().value());
            }
        }
    }

    @Test
    void scanAfterCompaction() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            tree.flush();

            awaitCompaction(tree);

            Slice startKey = key("key-020");
            Slice endKey = key("key-030");

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.between(startKey, endKey)));

            assertEquals(10, entries.size());
            for (StoredEntry.Value e : entries) {
                assertTrue(e.key().compareTo(startKey) >= 0);
                assertTrue(e.key().compareTo(endKey) < 0);
            }
        }
    }
}
