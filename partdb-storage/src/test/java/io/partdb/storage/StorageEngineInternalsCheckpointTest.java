package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsCheckpointTest extends StorageEngineInternalTestSupport {

    @Test
    void roundtrip() {
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpointBytes();
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
        try (StorageEngine source = StorageEngine.open(sourceDir, LsmConfig.defaults())) {
            put(source, key(1), value(10), nextRevision());
            put(source, key(2), value(20), nextRevision());
            source.flush();
            checkpoint = source.checkpointBytes();
        }

        try (StorageEngine restored = StorageEngine.open(restoredDir, LsmConfig.defaults())) {
            restored.replaceWithCheckpoint(checkpoint);

            assertTrue(restored.get(key(1)).isPresent());
            assertEquals(value(10), restored.get(key(1)).get().value());
            assertTrue(restored.get(key(2)).isPresent());
            assertEquals(value(20), restored.get(key(2)).get().value());
        }
    }

    @Test
    void restoresToPreviousState() {
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpointBytes();

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
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            put(tree, key(2), value(20), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpointBytes();
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
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpointBytes();

            put(tree, key(2), value(20), nextRevision());

            assertTrue(tree.get(key(2)).isPresent());

            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isEmpty());
        }
    }

    @Test
    void manifestStateRestored() {
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpointBytes();
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
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            tree.flush();
            byte[] checkpoint1 = tree.checkpointBytes();

            put(tree, key(2), value(20), nextRevision());
            tree.flush();
            byte[] checkpoint2 = tree.checkpointBytes();

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
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            byte[] checkpoint = tree.checkpointBytes();

            put(tree, key(1), value(10), nextRevision());
            tree.flush();
            assertTrue(tree.get(key(1)).isPresent());

            tree.replaceWithCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isEmpty());
            assertTrue(tree.manifest().sstables().isEmpty());
        }
    }
}
