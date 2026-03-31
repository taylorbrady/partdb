package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreRuntimeCompactionTest extends StoreRuntimeTestSupport {

    @Test
    void l0TriggersAtThreshold() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 100; i++) {
                tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }

            tree.flush();
            Thread.sleep(500);

            SSTableManifest manifest = tree.manifest();
            List<SSTableMetadata> l0Files = manifest.level(0);
            List<SSTableMetadata> l1Files = manifest.level(1);

            assertTrue(l0Files.size() < 4);
            assertTrue(l1Files.size() > 0);
        }
    }

    @Test
    void mergesOverlappingKeys() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int version = 0; version < 5; version++) {
                for (int i = 0; i < 20; i++) {
                    tree.put(key(String.format("key-%02d", i)), value("v" + version + "-" + i), nextRevision());
                }
                tree.flush();
            }

            Thread.sleep(1000);

            for (int i = 0; i < 20; i++) {
                Optional<EngineEntry> result = tree.get(key(String.format("key-%02d", i)));
                assertTrue(result.isPresent());
                assertTrue(new String(result.get().value().toByteArray(), StandardCharsets.UTF_8).startsWith("v4"));
            }
        }
    }

    @Test
    void tombstonesResultInEmptyGet() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 50; i++) {
                tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            tree.flush();

            for (int i = 0; i < 50; i++) {
                tree.delete(key(String.format("key-%03d", i)), nextRevision());
            }
            tree.flush();

            Thread.sleep(500);

            for (int i = 0; i < 50; i++) {
                assertTrue(tree.get(key(String.format("key-%03d", i))).isEmpty());
            }
        }
    }

    @Test
    void levelSizeRespected() throws Exception {
        LsmConfig config = smallMemtableConfig(2048);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int batch = 0; batch < 20; batch++) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(String.format("key-%05d", batch * 100 + i)), largeValue(100), nextRevision());
                }
                tree.flush();
            }

            Thread.sleep(2000);

            SSTableManifest manifest = tree.manifest();

            for (int level = 1; level < manifest.maxLevel(); level++) {
                long levelSize = manifest.levelSize(level);
                long maxSize = config.maxBytesForLevel(level);

                assertTrue(levelSize <= maxSize * 2);
            }
        }
    }

    @Test
    void preservesNewestVersions() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            List<String> expectedValues = new ArrayList<>();

            for (int i = 0; i < 30; i++) {
                String val = "version-" + i;
                expectedValues.add(val);
                tree.put(key(String.format("key-%02d", i)), value(val), nextRevision());

                if (i % 10 == 9) {
                    tree.flush();
                }
            }

            Thread.sleep(1000);

            for (int i = 0; i < 30; i++) {
                Optional<EngineEntry> result = tree.get(key(String.format("key-%02d", i)));
                assertTrue(result.isPresent());
                assertEquals(expectedValues.get(i), new String(result.get().value().toByteArray(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void manifestConsistency() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 80; i++) {
                tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            tree.flush();

            Thread.sleep(500);

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
    void reopenAfterCompaction() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        List<Slice> keys = new ArrayList<>();
        List<Slice> values = new ArrayList<>();

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 60; i++) {
                Slice k = key(String.format("key-%03d", i));
                Slice v = value("value-" + i);
                keys.add(k);
                values.add(v);
                tree.put(k, v, nextRevision());
            }
            tree.flush();
            Thread.sleep(500);
        }

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < keys.size(); i++) {
                Optional<EngineEntry> result = tree.get(keys.get(i));
                assertTrue(result.isPresent());
                assertEquals(values.get(i), result.get().value());
            }
        }
    }

    @Test
    void scanAfterCompaction() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 100; i++) {
                tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            tree.flush();

            Thread.sleep(500);

            Slice startKey = key("key-020");
            Slice endKey = key("key-030");

            List<EngineEntry> entries = readAll(tree.scan(startKey, endKey));

            assertEquals(10, entries.size());
            for (EngineEntry e : entries) {
                assertTrue(e.key().compareTo(startKey) >= 0);
                assertTrue(e.key().compareTo(endKey) < 0);
            }
        }
    }
}
