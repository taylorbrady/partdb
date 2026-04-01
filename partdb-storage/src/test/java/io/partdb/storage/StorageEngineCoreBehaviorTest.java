package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineCoreBehaviorTest extends StorageEngineCoreTestSupport {

    @Test
    void putAndGet() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());

            Optional<StoredEntry.Value> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(10), result.get().value());
        }
    }

    @Test
    void getNonExistentKey() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            Optional<StoredEntry.Value> result = tree.get(key(99));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteKey() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            delete(tree, key(1), nextRevision());

            Optional<StoredEntry.Value> result = tree.get(key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteNonExistentKey() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            delete(tree, key(1), nextRevision());

            Optional<StoredEntry.Value> result = tree.get(key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(1), value(20), nextRevision());

            Optional<StoredEntry.Value> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(20), result.get().value());
        }
    }

    @Test
    void prefersNewestImmutableMemtableValue() {
        MutableMemtable active = new MutableMemtable();
        MutableMemtable older = new MutableMemtable();
        MutableMemtable newer = new MutableMemtable();

        older.put(new StoredEntry.Value(key("key"), value("older"), 1));
        newer.put(new StoredEntry.Value(key("key"), value("newer"), 2));

        Optional<StoredEntry> result = ReadCoordinator.lookupStoredEntry(
            key("key"),
            active,
            List.of(older.freeze(), newer.freeze())
        );

        assertTrue(result.isPresent());
        assertInstanceOf(StoredEntry.Value.class, result.get());
        assertEquals(value("newer"), ((StoredEntry.Value) result.get()).value());
    }

    @Test
    void entireRange() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.all()));

            assertEquals(3, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(2), entries.get(1).key());
            assertEquals(key(3), entries.get(2).key());
        }
    }

    @Test
    void withBounds() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());
            put(tree, key(4), value(40), nextRevision());

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.between(key(2), key(4))));

            assertEquals(2, entries.size());
            assertEquals(key(2), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }
    }

    @Test
    void excludesDeletedKeys() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            delete(tree, key(2), nextRevision());
            put(tree, key(3), value(30), nextRevision());

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.all()));

            assertEquals(2, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }
    }

    @Test
    void mergesFromMultipleSources() {
        LsmConfig config = smallMemtableConfig(512);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            for (int i = 0; i < 20; i++) {
                put(tree, key(i), largeValue(100), nextRevision());
            }

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.all()));

            assertEquals(20, entries.size());
            for (int i = 0; i < 20; i++) {
                assertEquals(key(i), entries.get(i).key());
            }
        }
    }

    @Test
    void usesLatestValueForDuplicateKeys() {
        LsmConfig config = smallMemtableConfig(256);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), largeValue(100), nextRevision());
            put(tree, key(1), value(20), nextRevision());

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.all()));

            Optional<StoredEntry.Value> keyEntry = entries.stream()
                .filter(e -> e.key().equals(key(1)))
                .findFirst();

            assertTrue(keyEntry.isPresent());
            assertEquals(value(20), keyEntry.get().value());
        }
    }

    @Test
    void manualFlush() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());

            tree.flush();

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
        }
    }

    @Test
    void automaticOnMemtableSize() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            for (int i = 0; i < 10; i++) {
                put(tree, key(i), largeValue(200), nextRevision());
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(tree.get(key(i)).isPresent());
            }
        }
    }

    @Test
    void fromSSTables() {
        LsmConfig config = LsmConfig.defaults();

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            tree.flush();
        }

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            Optional<StoredEntry.Value> result1 = tree.get(key(1));
            Optional<StoredEntry.Value> result2 = tree.get(key(2));

            assertTrue(result1.isPresent());
            assertEquals(value(10), result1.get().value());

            assertTrue(result2.isPresent());
            assertEquals(value(20), result2.get().value());
        }
    }

    @Test
    void readPathPriority() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();
            put(tree, key(1), value(20), nextRevision());

            Optional<StoredEntry.Value> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(20), result.get().value());
        }
    }

    @Test
    void rejectsOlderRevisionThanPersistedValue() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), 10);
            tree.flush();

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> put(tree, key(1), value(20), 9)
            );

            assertTrue(error.getMessage().contains("older"));
            assertEquals(value(10), tree.get(key(1)).orElseThrow().value());
        }
    }

    @Test
    void multipleSSTables() {
        LsmConfig config = smallMemtableConfig(512);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            for (int i = 0; i < 30; i++) {
                put(tree, key(i), largeValue(100), nextRevision());
            }

            tree.flush();

            for (int i = 0; i < 30; i++) {
                assertTrue(tree.get(key(i)).isPresent());
            }
        }
    }

    @Test
    void emptyManifestLoad() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            SSTableManifest manifest = tree.manifest();
            assertNotNull(manifest);
            assertTrue(manifest.sstables().isEmpty());
        }
    }
}
