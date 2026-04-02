package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

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

    @Test
    void exactRestoreSequenceSurfacesRestoredTableThroughEngineAndReader() {
        LsmConfig config = StorageOptions.builder()
            .writeBufferMaxBytes(192)
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(192)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(384)
                .maxLevels(4)
                .build())
            .build()
            .toLsmConfig();

        Path dir0 = tempDir.resolve("mixed-store-0");
        Path dir1 = tempDir.resolve("mixed-store-1");
        Path dir2 = tempDir.resolve("mixed-store-2");

        byte[] empty;
        byte[] key23;

        try (StorageEngine store0 = StorageEngine.open(dir0, config)) {
            empty = store0.checkpointBytes();
            put(store0, key(7), value("v1"), 1);
            store0.replaceWithCheckpoint(empty);
            store0.replaceWithCheckpoint(empty);
            put(store0, key(23), value("v23"), 2);
            key23 = store0.checkpointBytes();
            put(store0, key(15), value("v15"), 3);
            put(store0, key(10), value("v10"), 4);
            put(store0, key(14), value("v14"), 5);
        }

        try (StorageEngine store1 = StorageEngine.open(dir1, config)) {
            store1.replaceWithCheckpoint(key23);
            delete(store1, key(20), 6);
        }

        try (StorageEngine store2 = StorageEngine.open(dir2, config)) {
            store2.replaceWithCheckpoint(empty);
            put(store2, key(21), value("v21"), 7);
            store2.checkpointBytes();
            put(store2, key(13), value("v13"), 8);
            put(store2, key(20), value("v20-r9"), 9);
            put(store2, key(23), value("v23-r10"), 10);
            put(store2, key(8), value("v8"), 11);
            put(store2, key(2), value("v2"), 12);
            put(store2, key(20), value("v20-r13"), 13);
            delete(store2, key(15), 14);
            put(store2, key(12), value("v12"), 15);

            store2.replaceWithCheckpoint(key23);

            assertEquals(List.of(1L), store2.manifest().sstables().stream().map(SSTableMetadata::id).toList());
            assertEquals(2, store2.metadata().appliedThrough().value());

            Optional<StoredEntry.Value> loaded = store2.get(key(23));
            assertTrue(loaded.isPresent());
            assertEquals(value("v23"), loaded.orElseThrow().value());

            List<StoredEntry.Value> visible = readAll(store2.scan(ScanBounds.between(key(0), key(25))));
            assertEquals(1, visible.size());
            assertEquals(key(23), visible.getFirst().key());

            SSTableMetadata metadata = store2.manifest().sstables().getFirst();
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

    @Test
    void randomizedPrefixAtFailingRestoreMatchesRawReaderState() {
        StorageOptions options = StorageOptions.builder()
            .writeBufferMaxBytes(192)
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(192)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(384)
                .maxLevels(4)
                .build())
            .build();

        Random random = new Random(67890);
        List<SnapshotState> snapshots = new ArrayList<>();
        TreeMap<Integer, BytesAndRevision> expected = new TreeMap<>();
        int nextStoreId = 1;
        long revision = 0;

        try (PublicStoreHolder holder = PublicStoreHolder.open(tempDir.resolve("mixed-store-0"), options)) {
            snapshots.add(new SnapshotState(holder.store.checkpoint(), new TreeMap<>(expected)));

            for (int step = 0; step <= 25; step++) {
                int op = random.nextInt(100);
                if (op < 38) {
                    int key = random.nextInt(24);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.put(io.partdb.bytes.Bytes.copyOf(new byte[]{(byte) key}), io.partdb.bytes.Bytes.utf8("v" + revision)));
                    expected.put(key, new BytesAndRevision(io.partdb.bytes.Bytes.utf8("v" + revision), revision));
                } else if (op < 56) {
                    int key = random.nextInt(24);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.delete(io.partdb.bytes.Bytes.copyOf(new byte[]{(byte) key})));
                    expected.remove(key);
                } else if (op < 68) {
                    int key = random.nextInt(24);
                    holder.store.get(io.partdb.bytes.Bytes.copyOf(new byte[]{(byte) key}));
                } else if (op < 80) {
                    int start = random.nextInt(24);
                    int end = random.nextInt(25);
                    try (Scan ignored = holder.store.scan(KeyRange.between(
                        io.partdb.bytes.Bytes.copyOf(new byte[]{(byte) Math.min(start, end)}),
                        io.partdb.bytes.Bytes.copyOf(new byte[]{(byte) Math.max(start, end)})
                    ))) {
                        for (EntryRecord ignoredEntry : ignored) {
                            // drain
                        }
                    }
                } else if (op < 88) {
                    snapshots.add(new SnapshotState(holder.store.checkpoint(), new TreeMap<>(expected)));
                } else if (op < 92) {
                    holder.reopen();
                } else if (op < 96) {
                    SnapshotState snapshot = snapshots.get(random.nextInt(snapshots.size()));
                    holder.store.restoreInPlace(snapshot.snapshot());
                    expected = new TreeMap<>(snapshot.visibleState());
                } else {
                    SnapshotState snapshot = snapshots.get(random.nextInt(snapshots.size()));
                    holder.replaceWith(tempDir.resolve("mixed-store-" + nextStoreId++), snapshot.snapshot());
                    expected = new TreeMap<>(snapshot.visibleState());
                }
            }

            assertEquals(List.of(23), new ArrayList<>(expected.keySet()));
            assertEquals(List.of(1L), holder.store.manifest().sstables().stream().map(SSTableMetadata::id).toList());

            Optional<ValueRecord> publicGet = holder.store.get(io.partdb.bytes.Bytes.copyOf(new byte[]{23}));
            Optional<StoredEntry.Value> internalGet = holder.store.get(key(23));
            List<StoredEntry.Value> visible = readAll(holder.store.scan(ScanBounds.between(key(0), key(25))));
            assertTrue(publicGet.isPresent());
            assertTrue(internalGet.isPresent());
            assertEquals(1, visible.size());
        }
    }

    private record SnapshotState(StorageCheckpoint snapshot, TreeMap<Integer, BytesAndRevision> visibleState) {}

    private record BytesAndRevision(io.partdb.bytes.Bytes value, long revision) {}

    private static final class PublicStoreHolder implements AutoCloseable {
        private final StorageOptions options;
        private StorageEngine store;
        private Path directory;

        private PublicStoreHolder(Path directory, StorageOptions options, StorageEngine store) {
            this.directory = directory;
            this.options = options;
            this.store = store;
        }

        static PublicStoreHolder open(Path directory, StorageOptions options) {
            return new PublicStoreHolder(directory, options, StorageEngine.open(directory, options));
        }

        void reopen() {
            store.close();
            store = StorageEngine.open(directory, options);
        }

        void replaceWith(Path directory, StorageCheckpoint checkpoint) {
            store.close();
            this.directory = directory;
            this.store = StorageEngine.restore(directory, checkpoint, options);
        }

        @Override
        public void close() {
            store.close();
        }
    }
}
