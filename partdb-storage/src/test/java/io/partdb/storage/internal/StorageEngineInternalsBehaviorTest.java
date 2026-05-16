package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsBehaviorTest extends StorageEngineInternalTestSupport {

    @Test
    void putAndGet() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());

            Optional<ValueRecord> result = get(tree, key(1));

            assertTrue(result.isPresent());
            assertEquals(bytes(value(10)), result.get().value());
        }
    }

    @Test
    void getNonExistentKey() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            Optional<ValueRecord> result = get(tree, key(99));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteKey() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            delete(tree, key(1), nextRevision());

            Optional<ValueRecord> result = get(tree, key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteNonExistentKey() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            delete(tree, key(1), nextRevision());

            Optional<ValueRecord> result = get(tree, key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(1), value(20), nextRevision());

            Optional<ValueRecord> result = get(tree, key(1));

            assertTrue(result.isPresent());
            assertEquals(bytes(value(20)), result.get().value());
        }
    }

    @Test
    void prefersNewestImmutableMemtableValue() {
        MutableMemtable active = new MutableMemtable();
        MutableMemtable older = new MutableMemtable();
        MutableMemtable newer = new MutableMemtable();

        older.put(new StoredEntry.Value(key("key"), value("older"), 1));
        newer.put(new StoredEntry.Value(key("key"), value("newer"), 2));

        Optional<StoredEntry> result = ReadView.lookupStoredEntry(
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
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.all()));

            assertEquals(3, entries.size());
            assertEquals(bytes(key(1)), entries.get(0).key());
            assertEquals(bytes(key(2)), entries.get(1).key());
            assertEquals(bytes(key(3)), entries.get(2).key());
        }
    }

    @Test
    void withBounds() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());
            put(tree, key(4), value(40), nextRevision());

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.between(bytes(key(2)), bytes(key(4)))));

            assertEquals(2, entries.size());
            assertEquals(bytes(key(2)), entries.get(0).key());
            assertEquals(bytes(key(3)), entries.get(1).key());
        }
    }

    @Test
    void excludesDeletedKeys() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            delete(tree, key(2), nextRevision());
            put(tree, key(3), value(30), nextRevision());

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.all()));

            assertEquals(2, entries.size());
            assertEquals(bytes(key(1)), entries.get(0).key());
            assertEquals(bytes(key(3)), entries.get(1).key());
        }
    }

    @Test
    void mergesFromMultipleSources() {
        StorageOptions options = smallWriteBufferOptions(512);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 20; i++) {
                put(tree, key(i), largeValue(100), nextRevision());
            }

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.all()));

            assertEquals(20, entries.size());
            for (int i = 0; i < 20; i++) {
                assertEquals(bytes(key(i)), entries.get(i).key());
            }
        }
    }

    @Test
    void usesLatestValueForDuplicateKeys() {
        StorageOptions options = smallWriteBufferOptions(256);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), largeValue(100), nextRevision());
            put(tree, key(1), value(20), nextRevision());

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.all()));

            Optional<EntryRecord> keyEntry = entries.stream()
                .filter(e -> e.key().equals(bytes(key(1))))
                .findFirst();

            assertTrue(keyEntry.isPresent());
            assertEquals(bytes(value(20)), keyEntry.get().value());
        }
    }

    @Test
    void manualFlush() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());

            drainToDurableState(tree);

            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isPresent());
        }
    }

    @Test
    void automaticOnMemtableSize() {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 10; i++) {
                put(tree, key(i), largeValue(200), nextRevision());
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(get(tree, key(i)).isPresent());
            }
        }
    }

    @Test
    void fromSSTables() {
        StorageOptions options = StorageOptions.defaults();

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);
        }

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            Optional<ValueRecord> result1 = get(tree, key(1));
            Optional<ValueRecord> result2 = get(tree, key(2));

            assertTrue(result1.isPresent());
            assertEquals(bytes(value(10)), result1.get().value());

            assertTrue(result2.isPresent());
            assertEquals(bytes(value(20)), result2.get().value());
        }
    }

    @Test
    void readPathPriority() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);
            put(tree, key(1), value(20), nextRevision());

            Optional<ValueRecord> result = get(tree, key(1));

            assertTrue(result.isPresent());
            assertEquals(bytes(value(20)), result.get().value());
        }
    }

    @Test
    void rejectsOlderRevisionThanPersistedValue() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), 10);
            drainToDurableState(tree);

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> put(tree, key(1), value(20), 9)
            );

            assertTrue(error.getMessage().contains("older"));
            assertEquals(bytes(value(10)), get(tree, key(1)).orElseThrow().value());
        }
    }

    @Test
    void multipleSSTables() {
        StorageOptions options = smallWriteBufferOptions(512);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 30; i++) {
                put(tree, key(i), largeValue(100), nextRevision());
            }

            drainToDurableState(tree);

            for (int i = 0; i < 30; i++) {
                assertTrue(get(tree, key(i)).isPresent());
            }
        }
    }

    @Test
    void emptyManifestLoad() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            SSTableManifest manifest = readManifest(tempDir);
            assertNotNull(manifest);
            assertTrue(manifest.sstables().isEmpty());
        }
    }
}
