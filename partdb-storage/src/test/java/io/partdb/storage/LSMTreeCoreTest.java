package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LSMTreeCoreTest extends LSMTreeTestSupport {

    @Test
    void putAndGet() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());

            Optional<StorageEntry> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(10), result.get().value());
        }
    }

    @Test
    void getNonExistentKey() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            Optional<StorageEntry> result = tree.get(key(99));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteKey() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.delete(key(1), nextRevision());

            Optional<StorageEntry> result = tree.get(key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void deleteNonExistentKey() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.delete(key(1), nextRevision());

            Optional<StorageEntry> result = tree.get(key(1));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void putOverwritesPreviousValue() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(1), value(20), nextRevision());

            Optional<StorageEntry> result = tree.get(key(1));

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

        Optional<Mutation> result = LSMTree.lookupMutation(key("key"), active, List.of(older, newer));

        assertTrue(result.isPresent());
        assertInstanceOf(Mutation.Put.class, result.get());
        assertEquals(value("newer"), ((Mutation.Put) result.get()).value());
    }

    @Test
    void entireRange() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.put(key(3), value(30), nextRevision());

            List<StorageEntry> entries = readAll(tree.scan(null, null));

            assertEquals(3, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(2), entries.get(1).key());
            assertEquals(key(3), entries.get(2).key());
        }
    }

    @Test
    void withBounds() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.put(key(3), value(30), nextRevision());
            tree.put(key(4), value(40), nextRevision());

            List<StorageEntry> entries = readAll(tree.scan(key(2), key(4)));

            assertEquals(2, entries.size());
            assertEquals(key(2), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }
    }

    @Test
    void excludesDeletedKeys() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.delete(key(2), nextRevision());
            tree.put(key(3), value(30), nextRevision());

            List<StorageEntry> entries = readAll(tree.scan(null, null));

            assertEquals(2, entries.size());
            assertEquals(key(1), entries.get(0).key());
            assertEquals(key(3), entries.get(1).key());
        }
    }

    @Test
    void mergesFromMultipleSources() {
        LSMConfig config = smallMemtableConfig(512);

        try (LSMTree tree = LSMTree.open(tempDir, config)) {
            for (int i = 0; i < 20; i++) {
                tree.put(key(i), largeValue(100), nextRevision());
            }

            List<StorageEntry> entries = readAll(tree.scan(null, null));

            assertEquals(20, entries.size());
            for (int i = 0; i < 20; i++) {
                assertEquals(key(i), entries.get(i).key());
            }
        }
    }

    @Test
    void usesLatestValueForDuplicateKeys() {
        LSMConfig config = smallMemtableConfig(256);

        try (LSMTree tree = LSMTree.open(tempDir, config)) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), largeValue(100), nextRevision());
            tree.put(key(1), value(20), nextRevision());

            List<StorageEntry> entries = readAll(tree.scan(null, null));

            Optional<StorageEntry> keyEntry = entries.stream()
                .filter(e -> e.key().equals(key(1)))
                .findFirst();

            assertTrue(keyEntry.isPresent());
            assertEquals(value(20), keyEntry.get().value());
        }
    }

    @Test
    void manualFlush() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());

            tree.flush();

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
        }
    }

    @Test
    void automaticOnMemtableSize() {
        LSMConfig config = smallMemtableConfig(1024);

        try (LSMTree tree = LSMTree.open(tempDir, config)) {
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
        LSMConfig config = LSMConfig.defaults();

        try (LSMTree tree = LSMTree.open(tempDir, config)) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.flush();
        }

        try (LSMTree tree = LSMTree.open(tempDir, config)) {
            Optional<StorageEntry> result1 = tree.get(key(1));
            Optional<StorageEntry> result2 = tree.get(key(2));

            assertTrue(result1.isPresent());
            assertEquals(value(10), result1.get().value());

            assertTrue(result2.isPresent());
            assertEquals(value(20), result2.get().value());
        }
    }

    @Test
    void readPathPriority() {
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();
            tree.put(key(1), value(20), nextRevision());

            Optional<StorageEntry> result = tree.get(key(1));

            assertTrue(result.isPresent());
            assertEquals(value(20), result.get().value());
        }
    }

    @Test
    void multipleSSTables() {
        LSMConfig config = smallMemtableConfig(512);

        try (LSMTree tree = LSMTree.open(tempDir, config)) {
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
        try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
            SSTableManifest manifest = tree.manifest();
            assertNotNull(manifest);
            assertTrue(manifest.sstables().isEmpty());
        }
    }
}
