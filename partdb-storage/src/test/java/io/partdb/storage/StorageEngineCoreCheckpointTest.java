package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineCoreCheckpointTest extends StorageEngineCoreTestSupport {

    @Test
    void roundtrip() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();
            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertEquals(value(10), tree.get(key(1)).get().value());
            assertTrue(tree.get(key(2)).isPresent());
            assertEquals(value(20), tree.get(key(2)).get().value());
        }
    }

    @Test
    void restoresIntoFreshDirectory() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        byte[] checkpoint;
        try (StorageEngineCore source = StorageEngineCore.open(sourceDir, LsmConfig.defaults())) {
            put(source, key(1), value(10), nextRevision());
            put(source, key(2), value(20), nextRevision());
            source.flush();
            checkpoint = source.checkpoint();
        }

        try (StorageEngineCore restored = StorageEngineCore.open(restoredDir, LsmConfig.defaults())) {
            restored.replaceWithCheckpoint(checkpoint);

            assertTrue(restored.get(key(1)).isPresent());
            assertEquals(value(10), restored.get(key(1)).get().value());
            assertTrue(restored.get(key(2)).isPresent());
            assertEquals(value(20), restored.get(key(2)).get().value());
        }
    }

    @Test
    void restoresToPreviousState() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();

            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());
            tree.flush();

            assertTrue(tree.get(key(2)).isPresent());
            assertTrue(tree.get(key(3)).isPresent());

            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertEquals(value(10), tree.get(key(1)).get().value());
            assertTrue(tree.get(key(2)).isEmpty());
            assertTrue(tree.get(key(3)).isEmpty());
        }
    }

    @Test
    void capturesMultipleSSTables() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            put(tree, key(2), value(20), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();
            assertEquals(2, tree.manifest().sstables().size());

            put(tree, key(3), value(30), nextRevision());
            tree.flush();

            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
            assertTrue(tree.get(key(3)).isEmpty());
            assertEquals(2, tree.manifest().sstables().size());
        }
    }

    @Test
    void clearsMemtableOnRestore() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();

            put(tree, key(2), value(20), nextRevision());

            assertTrue(tree.get(key(2)).isPresent());

            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isEmpty());
        }
    }

    @Test
    void manifestStateRestored() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();
            long originalNextId = tree.manifest().nextSSTableId();
            int originalSSTableCount = tree.manifest().sstables().size();

            put(tree, key(2), value(20), nextRevision());
            tree.flush();

            assertTrue(tree.manifest().nextSSTableId() > originalNextId);

            tree.replaceWithCheckpoint(checkpoint);

            assertEquals(originalNextId, tree.manifest().nextSSTableId());
            assertEquals(originalSSTableCount, tree.manifest().sstables().size());
        }
    }

    @Test
    void multipleCheckpoints() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();
            byte[] checkpoint1 = tree.checkpoint();

            put(tree, key(2), value(20), nextRevision());
            tree.flush();
            byte[] checkpoint2 = tree.checkpoint();

            put(tree, key(3), value(30), nextRevision());
            tree.flush();

            tree.replaceWithCheckpoint(checkpoint1);
            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isEmpty());
            assertTrue(tree.get(key(3)).isEmpty());

            tree.replaceWithCheckpoint(checkpoint2);
            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
            assertTrue(tree.get(key(3)).isEmpty());
        }
    }

    @Test
    void emptyTree() {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            byte[] checkpoint = tree.checkpoint();

            put(tree, key(1), value(10), nextRevision());
            tree.flush();
            assertTrue(tree.get(key(1)).isPresent());

            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isEmpty());
            assertTrue(tree.manifest().sstables().isEmpty());
        }
    }
}
