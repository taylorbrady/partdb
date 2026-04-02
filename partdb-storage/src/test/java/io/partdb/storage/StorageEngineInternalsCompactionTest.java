package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsCompactionTest extends StorageEngineInternalTestSupport {

    @Test
    void l0TriggersAtThreshold() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }

            drainToDurableState(tree);
            awaitCompaction(tree, () -> {
                SSTableManifest manifest = readManifest(tempDir);
                return manifest.level(0).size() < 4 && !manifest.level(1).isEmpty();
            });

            SSTableManifest manifest = readManifest(tempDir);
            List<SSTableMetadata> l0Files = manifest.level(0);
            List<SSTableMetadata> l1Files = manifest.level(1);

            assertTrue(l0Files.size() < 4);
            assertTrue(l1Files.size() > 0);
        }
    }

    @Test
    void mergesOverlappingKeys() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int version = 0; version < 5; version++) {
                for (int i = 0; i < 20; i++) {
                    put(tree, key(String.format("key-%02d", i)), value("v" + version + "-" + i), nextRevision());
                }
                drainToDurableState(tree);
            }

            awaitCompaction(tree, () -> {
                for (int i = 0; i < 20; i++) {
                    Optional<ValueRecord> result = get(tree, key(String.format("key-%02d", i)));
                    if (result.isEmpty() || !result.get().value().utf8().startsWith("v4")) {
                        return false;
                    }
                }
                return true;
            });

            for (int i = 0; i < 20; i++) {
                Optional<ValueRecord> result = get(tree, key(String.format("key-%02d", i)));
                assertTrue(result.isPresent());
                assertTrue(result.get().value().utf8().startsWith("v4"));
            }
        }
    }

    @Test
    void tombstonesResultInEmptyGet() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            drainToDurableState(tree);

            for (int i = 0; i < 50; i++) {
                delete(tree, key(String.format("key-%03d", i)), nextRevision());
            }
            drainToDurableState(tree);

            awaitCompaction(tree, () -> {
                for (int i = 0; i < 50; i++) {
                    if (get(tree, key(String.format("key-%03d", i))).isPresent()) {
                        return false;
                    }
                }
                return true;
            });

            for (int i = 0; i < 50; i++) {
                assertTrue(get(tree, key(String.format("key-%03d", i))).isEmpty());
            }
        }
    }

    @Test
    void levelSizeRespected() {
        StorageOptions options = smallWriteBufferOptions(2048);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int batch = 0; batch < 20; batch++) {
                for (int i = 0; i < 100; i++) {
                    put(tree, key(String.format("key-%05d", batch * 100 + i)), largeValue(100), nextRevision());
                }
                drainToDurableState(tree);
            }

            awaitCompaction(tree, () -> {
                SSTableManifest manifest = readManifest(tempDir);
                for (int level = 1; level < manifest.maxLevel(); level++) {
                    if (manifest.levelSize(level) > maxBytesForLevel(options, level) * 2) {
                        return false;
                    }
                }
                return true;
            });

            SSTableManifest manifest = readManifest(tempDir);

            for (int level = 1; level < manifest.maxLevel(); level++) {
                long levelSize = manifest.levelSize(level);
                long maxSize = maxBytesForLevel(options, level);

                assertTrue(levelSize <= maxSize * 2);
            }
        }
    }

    @Test
    void preservesNewestVersions() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            List<String> expectedValues = new ArrayList<>();

            for (int i = 0; i < 30; i++) {
                String val = "version-" + i;
                expectedValues.add(val);
                put(tree, key(String.format("key-%02d", i)), value(val), nextRevision());

                if (i % 10 == 9) {
                    drainToDurableState(tree);
                }
            }

            awaitCompaction(tree, () -> {
                for (int i = 0; i < 30; i++) {
                    Optional<ValueRecord> result = get(tree, key(String.format("key-%02d", i)));
                    if (result.isEmpty() || !expectedValues.get(i).equals(result.get().value().utf8())) {
                        return false;
                    }
                }
                return true;
            });

            for (int i = 0; i < 30; i++) {
                Optional<ValueRecord> result = get(tree, key(String.format("key-%02d", i)));
                assertTrue(result.isPresent());
                assertEquals(expectedValues.get(i), result.get().value().utf8());
            }
        }
    }

    @Test
    void manifestConsistency() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 80; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            drainToDurableState(tree);

            awaitCompaction(tree, () -> !readManifest(tempDir).sstables().isEmpty());

            SSTableManifest manifest = readManifest(tempDir);

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
        StorageOptions options = smallWriteBufferOptions(1024);

        List<Slice> keys = new ArrayList<>();
        List<Slice> values = new ArrayList<>();

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 60; i++) {
                Slice k = key(String.format("key-%03d", i));
                Slice v = value("value-" + i);
                keys.add(k);
                values.add(v);
                put(tree, k, v, nextRevision());
            }
            drainToDurableState(tree);
            awaitCompaction(tree, () -> true);
        }

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < keys.size(); i++) {
                Optional<ValueRecord> result = get(tree, keys.get(i));
                assertTrue(result.isPresent());
                assertEquals(bytes(values.get(i)), result.get().value());
            }
        }
    }

    @Test
    void scanAfterCompaction() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
            }
            drainToDurableState(tree);

            awaitCompaction(tree, () -> true);

            Slice startKey = key("key-020");
            Slice endKey = key("key-030");

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.between(bytes(startKey), bytes(endKey))));

            assertEquals(10, entries.size());
            for (EntryRecord e : entries) {
                assertTrue(e.key().compareTo(bytes(startKey)) >= 0);
                assertTrue(e.key().compareTo(bytes(endKey)) < 0);
            }
        }
    }
}
