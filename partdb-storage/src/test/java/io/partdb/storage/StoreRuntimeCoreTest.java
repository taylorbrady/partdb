package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreRuntimeCoreTest extends StoreRuntimeTestSupport {

    @Test
    void putAndGet() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());

            Optional<EngineEntry> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(10), result.get().value());
        }
    }

    @Test
    void getNonExistentKey() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            Optional<EngineEntry> result = tree.get(key(99));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteKey() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.delete(key(1), nextRevision());

            Optional<EngineEntry> result = tree.get(key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteNonExistentKey() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.delete(key(1), nextRevision());

            Optional<EngineEntry> result = tree.get(key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(1), value(20), nextRevision());

            Optional<EngineEntry> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(20), result.get().value());
        }
    }

    @Test
    void prefersNewestImmutableMemtableValue() {
        Memtable active = new Memtable();
        Memtable older = new Memtable();
        Memtable newer = new Memtable();

        older.put(new Mutation.Put(key("key"), value("older"), 1));
        newer.put(new Mutation.Put(key("key"), value("newer"), 2));

        Optional<Mutation> result = StoreRuntime.lookupMutation(key("key"), active, List.of(older, newer));

        assertTrue(result.isPresent());
        assertInstanceOf(Mutation.Put.class, result.get());
        assertEquals(value("newer"), ((Mutation.Put) result.get()).value());
    }

    @Test
    void entireRange() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.put(key(3), value(30), nextRevision());

            List<EngineEntry> entries = readAll(tree.scan(null, null));

            assertEquals(3, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(2), entries.get(1).key());
            assertEquals(key(3), entries.get(2).key());
        }
    }

    @Test
    void withBounds() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.put(key(3), value(30), nextRevision());
            tree.put(key(4), value(40), nextRevision());

            List<EngineEntry> entries = readAll(tree.scan(key(2), key(4)));

            assertEquals(2, entries.size());
            assertEquals(key(2), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }
    }

    @Test
    void excludesDeletedKeys() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.delete(key(2), nextRevision());
            tree.put(key(3), value(30), nextRevision());

            List<EngineEntry> entries = readAll(tree.scan(null, null));

            assertEquals(2, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }
    }

    @Test
    void mergesFromMultipleSources() {
        LsmConfig config = smallMemtableConfig(512);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 20; i++) {
                tree.put(key(i), largeValue(100), nextRevision());
            }

            List<EngineEntry> entries = readAll(tree.scan(null, null));

            assertEquals(20, entries.size());
            for (int i = 0; i < 20; i++) {
                assertEquals(key(i), entries.get(i).key());
            }
        }
    }

    @Test
    void usesLatestValueForDuplicateKeys() {
        LsmConfig config = smallMemtableConfig(256);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), largeValue(100), nextRevision());
            tree.put(key(1), value(20), nextRevision());

            List<EngineEntry> entries = readAll(tree.scan(null, null));

            Optional<EngineEntry> keyEntry = entries.stream()
                .filter(e -> e.key().equals(key(1)))
                .findFirst();

            assertTrue(keyEntry.isPresent());
            assertEquals(value(20), keyEntry.get().value());
        }
    }

    @Test
    void manualFlush() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());

            tree.flush();

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
        }
    }

    @Test
    void automaticOnMemtableSize() {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 10; i++) {
                tree.put(key(i), largeValue(200), nextRevision());
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(tree.get(key(i)).isPresent());
            }
        }
    }

    @Test
    void fromSSTables() {
        LsmConfig config = LsmConfig.defaults();

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.flush();
        }

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            Optional<EngineEntry> result1 = tree.get(key(1));
            Optional<EngineEntry> result2 = tree.get(key(2));

            assertTrue(result1.isPresent());
            assertEquals(value(10), result1.get().value());

            assertTrue(result2.isPresent());
            assertEquals(value(20), result2.get().value());
        }
    }

    @Test
    void readPathPriority() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();
            tree.put(key(1), value(20), nextRevision());

            Optional<EngineEntry> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(20), result.get().value());
        }
    }

    @Test
    void rejectsOlderRevisionThanPersistedValue() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), 10);
            tree.flush();

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> tree.put(key(1), value(20), 9)
            );

            assertTrue(error.getMessage().contains("older"));
            assertEquals(value(10), tree.get(key(1)).orElseThrow().value());
        }
    }

    @Test
    void multipleSSTables() {
        LsmConfig config = smallMemtableConfig(512);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int i = 0; i < 30; i++) {
                tree.put(key(i), largeValue(100), nextRevision());
            }

            tree.flush();

            for (int i = 0; i < 30; i++) {
                assertTrue(tree.get(key(i)).isPresent());
            }
        }
    }

    @Test
    void emptyManifestLoad() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            SSTableManifest manifest = tree.manifest();
            assertNotNull(manifest);
            assertTrue(manifest.sstables().isEmpty());
        }
    }
}
