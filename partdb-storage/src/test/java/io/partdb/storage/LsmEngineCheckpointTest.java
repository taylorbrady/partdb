package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LsmEngineCheckpointTest extends LsmEngineTestSupport {

    @Test
    void roundtrip() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.put(key(2), value(20), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();
            tree.restoreFromCheckpoint(checkpoint);

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
        try (LsmEngine source = LsmEngine.open(sourceDir, LsmConfig.defaults())) {
            source.put(key(1), value(10), nextRevision());
            source.put(key(2), value(20), nextRevision());
            source.flush();
            checkpoint = source.checkpoint();
        }

        try (LsmEngine restored = LsmEngine.open(restoredDir, LsmConfig.defaults())) {
            restored.restoreFromCheckpoint(checkpoint);

            assertTrue(restored.get(key(1)).isPresent());
            assertEquals(value(10), restored.get(key(1)).get().value());
            assertTrue(restored.get(key(2)).isPresent());
            assertEquals(value(20), restored.get(key(2)).get().value());
        }
    }

    @Test
    void restoresToPreviousState() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();

            tree.put(key(2), value(20), nextRevision());
            tree.put(key(3), value(30), nextRevision());
            tree.flush();

            assertTrue(tree.get(key(2)).isPresent());
            assertTrue(tree.get(key(3)).isPresent());

            tree.restoreFromCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertEquals(value(10), tree.get(key(1)).get().value());
            assertTrue(tree.get(key(2)).isEmpty());
            assertTrue(tree.get(key(3)).isEmpty());
        }
    }

    @Test
    void capturesMultipleSSTables() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();

            tree.put(key(2), value(20), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();
            assertEquals(2, tree.manifest().sstables().size());

            tree.put(key(3), value(30), nextRevision());
            tree.flush();

            tree.restoreFromCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
            assertTrue(tree.get(key(3)).isEmpty());
            assertEquals(2, tree.manifest().sstables().size());
        }
    }

    @Test
    void clearsMemtableOnRestore() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();

            tree.put(key(2), value(20), nextRevision());

            assertTrue(tree.get(key(2)).isPresent());

            tree.restoreFromCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isEmpty());
        }
    }

    @Test
    void manifestStateRestored() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();

            byte[] checkpoint = tree.checkpoint();
            long originalNextId = tree.manifest().nextSSTableId();
            int originalSSTableCount = tree.manifest().sstables().size();

            tree.put(key(2), value(20), nextRevision());
            tree.flush();

            assertTrue(tree.manifest().nextSSTableId() > originalNextId);

            tree.restoreFromCheckpoint(checkpoint);

            assertEquals(originalNextId, tree.manifest().nextSSTableId());
            assertEquals(originalSSTableCount, tree.manifest().sstables().size());
        }
    }

    @Test
    void multipleCheckpoints() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            tree.put(key(1), value(10), nextRevision());
            tree.flush();
            byte[] checkpoint1 = tree.checkpoint();

            tree.put(key(2), value(20), nextRevision());
            tree.flush();
            byte[] checkpoint2 = tree.checkpoint();

            tree.put(key(3), value(30), nextRevision());
            tree.flush();

            tree.restoreFromCheckpoint(checkpoint1);
            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isEmpty());
            assertTrue(tree.get(key(3)).isEmpty());

            tree.restoreFromCheckpoint(checkpoint2);
            assertTrue(tree.get(key(1)).isPresent());
            assertTrue(tree.get(key(2)).isPresent());
            assertTrue(tree.get(key(3)).isEmpty());
        }
    }

    @Test
    void emptyTree() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            byte[] checkpoint = tree.checkpoint();

            tree.put(key(1), value(10), nextRevision());
            tree.flush();
            assertTrue(tree.get(key(1)).isPresent());

            tree.restoreFromCheckpoint(checkpoint);

            assertTrue(tree.get(key(1)).isEmpty());
            assertTrue(tree.manifest().sstables().isEmpty());
        }
    }
}
