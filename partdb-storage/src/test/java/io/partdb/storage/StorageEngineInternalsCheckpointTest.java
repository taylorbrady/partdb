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
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);

            StorageCheckpoint checkpoint = tree.checkpoint();
            tree.restore(checkpoint);

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

        StorageCheckpoint checkpoint;
        try (StorageEngine source = StorageEngine.open(sourceDir, StorageOptions.defaults())) {
            put(source, key(1), value(10), nextRevision());
            put(source, key(2), value(20), nextRevision());
            drainToDurableState(source);
            checkpoint = source.checkpoint();
        }

        try (StorageEngine restored = StorageEngine.openFromCheckpoint(restoredDir, checkpoint, StorageOptions.defaults())) {
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

            StorageCheckpoint checkpoint = tree.checkpoint();

            put(tree, key(2), value(20), nextRevision());
            put(tree, key(3), value(30), nextRevision());
            drainToDurableState(tree);

            assertTrue(get(tree, key(2)).isPresent());
            assertTrue(get(tree, key(3)).isPresent());

            tree.restore(checkpoint);

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

            StorageCheckpoint checkpoint = tree.checkpoint();
            assertEquals(2, readManifest(tempDir).sstables().size());

            put(tree, key(3), value(30), nextRevision());
            drainToDurableState(tree);

            tree.restore(checkpoint);

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

            StorageCheckpoint checkpoint = tree.checkpoint();

            put(tree, key(2), value(20), nextRevision());

            assertTrue(get(tree, key(2)).isPresent());

            tree.restore(checkpoint);

            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isEmpty());
        }
    }

    @Test
    void manifestStateRestored() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);

            StorageCheckpoint checkpoint = tree.checkpoint();
            SSTableManifest originalManifest = readManifest(tempDir);
            long originalNextId = originalManifest.nextSSTableId();
            int originalSstableCount = originalManifest.sstables().size();

            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);

            assertTrue(readManifest(tempDir).nextSSTableId() > originalNextId);

            tree.restore(checkpoint);

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
            StorageCheckpoint checkpoint1 = tree.checkpoint();

            put(tree, key(2), value(20), nextRevision());
            drainToDurableState(tree);
            StorageCheckpoint checkpoint2 = tree.checkpoint();

            put(tree, key(3), value(30), nextRevision());
            drainToDurableState(tree);

            tree.restore(checkpoint1);
            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isEmpty());
            assertTrue(get(tree, key(3)).isEmpty());

            tree.restore(checkpoint2);
            assertTrue(get(tree, key(1)).isPresent());
            assertTrue(get(tree, key(2)).isPresent());
            assertTrue(get(tree, key(3)).isEmpty());
        }
    }

    @Test
    void emptyTree() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            StorageCheckpoint checkpoint = tree.checkpoint();

            put(tree, key(1), value(10), nextRevision());
            drainToDurableState(tree);
            assertTrue(get(tree, key(1)).isPresent());

            tree.restore(checkpoint);

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

        StorageCheckpoint empty;
        StorageCheckpoint key23;

        try (StorageEngine store0 = StorageEngine.open(dir0, options)) {
            empty = store0.checkpoint();
            put(store0, key(7), value("v1"), 1);
            store0.restore(empty);
            store0.restore(empty);
            put(store0, key(23), value("v23"), 2);
            key23 = store0.checkpoint();
            put(store0, key(15), value("v15"), 3);
            put(store0, key(10), value("v10"), 4);
            put(store0, key(14), value("v14"), 5);
        }

        try (StorageEngine store1 = StorageEngine.openFromCheckpoint(dir1, key23, options)) {
            delete(store1, key(20), 6);
        }

        try (StorageEngine store2 = StorageEngine.openFromCheckpoint(dir2, empty, options)) {
            put(store2, key(21), value("v21"), 7);
            store2.checkpoint();
            put(store2, key(13), value("v13"), 8);
            put(store2, key(20), value("v20-r9"), 9);
            put(store2, key(23), value("v23-r10"), 10);
            put(store2, key(8), value("v8"), 11);
            put(store2, key(2), value("v2"), 12);
            put(store2, key(20), value("v20-r13"), 13);
            delete(store2, key(15), 14);
            put(store2, key(12), value("v12"), 15);

            store2.restore(key23);

            SSTableManifest manifest = readManifest(dir2);
            assertEquals(List.of(1L), manifest.sstables().stream().map(SSTableMetadata::id).toList());
            assertEquals(2, store2.metadata().appliedThrough().value());

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
                    holder.store.restore(snapshot.snapshot());
                    expected = new TreeMap<>(snapshot.visibleState());
                } else {
                    SnapshotState snapshot = snapshots.get(random.nextInt(snapshots.size()));
                    holder.replaceWith(tempDir.resolve("mixed-store-" + nextStoreId++), snapshot.snapshot());
                    expected = new TreeMap<>(snapshot.visibleState());
                }
            }

            assertEquals(List.of(23), new ArrayList<>(expected.keySet()));
            assertEquals(List.of(1L), readManifest(holder.directory).sstables().stream().map(SSTableMetadata::id).toList());

            Optional<ValueRecord> publicGet = holder.store.get(io.partdb.bytes.Bytes.copyOf(new byte[]{23}));
            List<EntryRecord> visible = readAll(holder.store.scan(KeyRange.between(bytes(key(0)), bytes(key(25)))));
            assertTrue(publicGet.isPresent());
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
            this.store = StorageEngine.openFromCheckpoint(directory, checkpoint, options);
        }

        @Override
        public void close() {
            store.close();
        }
    }
}
