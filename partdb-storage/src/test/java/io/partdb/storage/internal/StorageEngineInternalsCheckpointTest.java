package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsCheckpointTest extends StorageEngineInternalTestSupport {

    @Test
    void roundtrip() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);

            byte[] checkpoint = Checkpoints.capture(tree);
            Checkpoints.restore(tree, checkpoint);

            assertTrue(get(tree, key(1)).isPresent());
            assertEquals(bytes(value(10)), get(tree, key(1)).orElseThrow().value());
            assertTrue(get(tree, key(2)).isPresent());
            assertEquals(bytes(value(20)), get(tree, key(2)).orElseThrow().value());
        }
    }

    @Test
    void restoresIntoFreshDirectory() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        byte[] checkpoint;
        try (StorageEngine source = StorageEngine.open(sourceDir, StorageOptions.defaults())) {
            put(source, key(1), value(10), nextRevision());
            put(source, key(2), value(20), nextRevision());
            drainToDurableState(source);
            checkpoint = Checkpoints.capture(source);
        }

        try (StorageEngine restored = StorageEngine.open(restoredDir, StorageOptions.defaults())) {
            Checkpoints.restore(restored, checkpoint);
            assertTrue(get(restored, key(1)).isPresent());
            assertEquals(bytes(value(10)), get(restored, key(1)).orElseThrow().value());
            assertTrue(get(restored, key(2)).isPresent());
            assertEquals(bytes(value(20)), get(restored, key(2)).orElseThrow().value());
        }
    }

    @Test
    void restoresToPreviousState() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);

            byte[] checkpoint = Checkpoints.capture(tree);

            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());
            drainToDurableState(tree);

            assertTrue(get(tree, key(2)).isPresent());
            assertTrue(get(tree, key(3)).isPresent());

            Checkpoints.restore(tree, checkpoint);

            assertTrue(get(tree, key(1)).isPresent());
            assertEquals(bytes(value(10)), get(tree, key(1)).orElseThrow().value());
            assertTrue(get(tree, key(2)).isEmpty());
            assertTrue(get(tree, key(3)).isEmpty());
        }
    }

    @Test
    void capturesMultipleSSTables() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);

            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);

            byte[] checkpoint = Checkpoints.capture(tree);
            assertEquals(2, readManifest(tempDir).sstables().size());

            put(tree, key(3), value(30), nextRevision());
            drainToDurableState(tree);

            Checkpoints.restore(tree, checkpoint);

            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isPresent());
            assertTrue(get(tree, key(3)).isEmpty());
            assertEquals(2, readManifest(tempDir).sstables().size());
        }
    }

    @Test
    void clearsMemtableOnRestore() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);

            byte[] checkpoint = Checkpoints.capture(tree);

            put(tree, key(2), value(20), nextRevision());

            assertTrue(get(tree, key(2)).isPresent());

            Checkpoints.restore(tree, checkpoint);

            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isEmpty());
        }
    }

    @Test
    void manifestStateRestored() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);

            byte[] checkpoint = Checkpoints.capture(tree);
            SSTableManifest originalManifest = readManifest(tempDir);
            long originalNextId = originalManifest.nextSSTableId();
            int originalSstableCount = originalManifest.sstables().size();

            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);

            assertTrue(readManifest(tempDir).nextSSTableId() > originalNextId);

            Checkpoints.restore(tree, checkpoint);

            SSTableManifest restoredManifest = readManifest(tempDir);
            assertEquals(originalNextId, restoredManifest.nextSSTableId());
            assertEquals(originalSstableCount, restoredManifest.sstables().size());
        }
    }

    @Test
    void multipleCheckpoints() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);
            byte[] checkpoint1 = Checkpoints.capture(tree);

            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);
            byte[] checkpoint2 = Checkpoints.capture(tree);

            put(tree, key(3), value(30), nextRevision());
            drainToDurableState(tree);

            Checkpoints.restore(tree, checkpoint1);
            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isEmpty());
            assertTrue(get(tree, key(3)).isEmpty());

            Checkpoints.restore(tree, checkpoint2);
            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isPresent());
            assertTrue(get(tree, key(3)).isEmpty());
        }
    }

    @Test
    void emptyTree() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            byte[] checkpoint = Checkpoints.capture(tree);

            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);
            assertTrue(get(tree, key(1)).isPresent());

            Checkpoints.restore(tree, checkpoint);

            assertTrue(get(tree, key(1)).isEmpty());
            assertTrue(readManifest(tempDir).sstables().isEmpty());
        }
    }

    @Test
    void exactRestoreSequenceSurfacesRestoredTableThroughEngineAndReader() {
        StorageOptions options = StorageOptions.builder()
            .writeBufferMaxBytes(192)
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(192)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(384)
                .maxLevels(4)
                .build())
            .build();

        Path dir0 = tempDir.resolve("mixed-store-0");
        Path dir1 = tempDir.resolve("mixed-store-1");
        Path dir2 = tempDir.resolve("mixed-store-2");

        byte[] empty;
        byte[] key23;

        try (StorageEngine store0 = StorageEngine.open(dir0, options)) {
            empty = Checkpoints.capture(store0);
            put(store0, key(7), value("v1"), 1);
            Checkpoints.restore(store0, empty);
            Checkpoints.restore(store0, empty);
            put(store0, key(23), value("v23"), 2);
            key23 = Checkpoints.capture(store0);
            put(store0, key(15), value("v15"), 3);
            put(store0, key(10), value("v10"), 4);
            put(store0, key(14), value("v14"), 5);
        }

        try (StorageEngine store1 = StorageEngine.open(dir1, options)) {
            Checkpoints.restore(store1, key23);
            delete(store1, key(20), 6);
        }

        try (StorageEngine store2 = StorageEngine.open(dir2, options)) {
            Checkpoints.restore(store2, empty);
            put(store2, key(21), value("v21"), 7);
            store2.flush();
            put(store2, key(13), value("v13"), 8);
            put(store2, key(20), value("v20-r9"), 9);
            put(store2, key(23), value("v23-r10"), 10);
            put(store2, key(8), value("v8"), 11);
            put(store2, key(2), value("v2"), 12);
            put(store2, key(20), value("v20-r13"), 13);
            delete(store2, key(15), 14);
            put(store2, key(12), value("v12"), 15);

            Checkpoints.restore(store2, key23);

            SSTableManifest manifest = readManifest(dir2);
            assertEquals(List.of(1L), manifest.sstables().stream().map(SSTableMetadata::id).toList());
            assertEquals(2, store2.appliedThrough().value());

            Optional<ValueRecord> loaded = get(store2, key(23));
            assertTrue(loaded.isPresent());
            assertEquals(bytes(value("v23")), loaded.orElseThrow().value());

            List<EntryRecord> visible = readAll(store2.scan(KeyRange.between(bytes(key(0)), bytes(key(25)))));
            assertEquals(1, visible.size());
            assertEquals(bytes(key(23)), visible.getFirst().key());

            SSTableMetadata metadata = manifest.sstables().getFirst();
            try (SSTableReader reader = SSTableReader.open(
                metadata.id(),
                metadata.level(),
                dir2.resolve("%06d.sst".formatted(metadata.id())),
                NoOpBlockCache.INSTANCE
            )) {
                assertTrue(reader.get(key(23), 2).isPresent());

                List<InternalEntry> tableEntries = new ArrayList<>();
                Iterator<InternalEntry> scan = reader.scan(ScanBounds.between(key(0), key(25)));
                while (scan.hasNext()) {
                    tableEntries.add(scan.next());
                }

                assertEquals(1, tableEntries.size());
                assertEquals(key(23), tableEntries.getFirst().userKey());
            }
        }
    }
}
