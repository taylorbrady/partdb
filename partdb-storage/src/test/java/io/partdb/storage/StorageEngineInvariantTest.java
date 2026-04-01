package io.partdb.storage;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInvariantTest {

    @TempDir
    Path tempDir;

    @Test
    void randomizedOperationsPreserveVisibleStateAcrossReopenAndRestore() {
        StorageOptions config = StorageOptions.builder()
            .writeBufferMaxBytes(256)
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(256)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(512)
                .maxLevels(4)
                .build())
            .build();

        Random random = new Random(12345);
        NavigableMap<Integer, ModelValue> expected = new TreeMap<>();
        Path currentDir = tempDir.resolve("store-0");
        int nextStoreId = 1;
        long revision = 0;
        List<String> operations = new ArrayList<>();

        try (StoreHolder holder = StoreHolder.open(currentDir, config)) {
            for (int step = 0; step < 300; step++) {
                int op = random.nextInt(100);
                if (op < 45) {
                    int key = random.nextInt(24);
                    Bytes value = valueFor(step, key);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.put(key(key), value));
                    expected.put(key, new ModelValue(value, revision));
                    operations.add("put step=%d key=%d rev=%d".formatted(step, key, revision));
                } else if (op < 65) {
                    int key = random.nextInt(24);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.delete(key(key)));
                    expected.remove(key);
                    operations.add("delete step=%d key=%d rev=%d".formatted(step, key, revision));
                } else if (op < 80) {
                    int key = random.nextInt(24);
                    operations.add("get step=%d key=%d".formatted(step, key));
                    assertGetMatches(expected, holder.store, key, operations);
                } else if (op < 90) {
                    int start = random.nextInt(24);
                    int end = random.nextInt(25);
                    operations.add("scan step=%d start=%d end=%d".formatted(step, start, end));
                    assertScanMatches(expected, holder.store, start, end, operations);
                } else if (op < 95) {
                    holder.reopen();
                    operations.add("reopen step=%d".formatted(step));
                } else {
                    StorageCheckpoint checkpoint = holder.store.checkpoint();
                    Path restoredDir = tempDir.resolve("store-" + nextStoreId++);
                    holder.replaceWith(restoredDir, checkpoint);
                    operations.add("restore step=%d dir=%s".formatted(step, restoredDir.getFileName()));
                }
            }

            operations.add("final-scan");
            assertScanMatches(expected, holder.store, 0, 25, operations);
        }
    }

    @Test
    void inPlaceRestoreRewindsVisibleStateAfterAggressiveCompaction() {
        StorageOptions config = StorageOptions.builder()
            .writeBufferMaxBytes(128)
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(128)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(256)
                .maxLevels(4)
                .build())
            .build();

        NavigableMap<Integer, ModelValue> expected = new TreeMap<>();
        List<String> operations = new ArrayList<>();
        long revision = 0;

        try (StoreHolder holder = StoreHolder.open(tempDir.resolve("store-in-place"), config)) {
            for (int round = 0; round < 80; round++) {
                int key = round % 16;
                Bytes value = valueFor(round, key);
                revision++;
                holder.store.apply(new Revision(revision), Mutation.put(key(key), value));
                expected.put(key, new ModelValue(value, revision));
                operations.add("seed-put round=%d key=%d rev=%d".formatted(round, key, revision));
            }

            for (int key = 0; key < 8; key++) {
                revision++;
                holder.store.apply(new Revision(revision), Mutation.delete(key(key)));
                expected.remove(key);
                operations.add("seed-delete key=%d rev=%d".formatted(key, revision));
            }

            StorageCheckpoint checkpoint = holder.store.checkpoint();
            NavigableMap<Integer, ModelValue> snapshotState = new TreeMap<>(expected);
            operations.add("snapshot");

            for (int round = 0; round < 240; round++) {
                int key = (round * 7) % 24;
                if (round % 5 == 0) {
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.delete(key(key)));
                    expected.remove(key);
                    operations.add("mutate-delete round=%d key=%d rev=%d".formatted(round, key, revision));
                } else {
                    Bytes value = valueFor(200 + round, key);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.put(key(key), value));
                    expected.put(key, new ModelValue(value, revision));
                    operations.add("mutate-put round=%d key=%d rev=%d".formatted(round, key, revision));
                }
            }

            holder.restoreInPlace(checkpoint);
            operations.add("restore-in-place");
            assertScanMatches(snapshotState, holder.store, 0, 25, operations);

            holder.reopen();
            operations.add("reopen-after-restore");
            assertScanMatches(snapshotState, holder.store, 0, 25, operations);
        }
    }

    @Test
    void randomizedSnapshotsSupportInPlaceAndCrossDirectoryRestore() {
        StorageOptions config = StorageOptions.builder()
            .writeBufferMaxBytes(192)
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(192)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(384)
                .maxLevels(4)
                .build())
            .build();

        Random random = new Random(67890);
        NavigableMap<Integer, ModelValue> expected = new TreeMap<>();
        List<SnapshotState> snapshots = new ArrayList<>();
        List<String> operations = new ArrayList<>();
        int nextStoreId = 1;
        long revision = 0;

        try (StoreHolder holder = StoreHolder.open(tempDir.resolve("mixed-store-0"), config)) {
            snapshots.add(new SnapshotState(holder.store.checkpoint(), copyState(expected)));
            operations.add("snapshot-initial");

            for (int step = 0; step < 350; step++) {
                int op = random.nextInt(100);
                if (op < 38) {
                    int key = random.nextInt(24);
                    Bytes value = valueFor(step, key);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.put(key(key), value));
                    expected.put(key, new ModelValue(value, revision));
                    operations.add("put step=%d key=%d rev=%d".formatted(step, key, revision));
                } else if (op < 56) {
                    int key = random.nextInt(24);
                    revision++;
                    holder.store.apply(new Revision(revision), Mutation.delete(key(key)));
                    expected.remove(key);
                    operations.add("delete step=%d key=%d rev=%d".formatted(step, key, revision));
                } else if (op < 68) {
                    int key = random.nextInt(24);
                    operations.add("get step=%d key=%d".formatted(step, key));
                    assertGetMatches(expected, holder.store, key, operations);
                } else if (op < 80) {
                    int start = random.nextInt(24);
                    int end = random.nextInt(25);
                    operations.add("scan step=%d start=%d end=%d".formatted(step, start, end));
                    assertScanMatches(expected, holder.store, start, end, operations);
                } else if (op < 88) {
                    snapshots.add(new SnapshotState(holder.store.checkpoint(), copyState(expected)));
                    operations.add("snapshot step=%d count=%d".formatted(step, snapshots.size()));
                } else if (op < 92) {
                    holder.reopen();
                    operations.add("reopen step=%d".formatted(step));
                    assertScanMatches(expected, holder.store, 0, 25, operations);
                } else if (op < 96) {
                    SnapshotState snapshot = snapshots.get(random.nextInt(snapshots.size()));
                    holder.restoreInPlace(snapshot.snapshot());
                    expected = copyState(snapshot.visibleState());
                    operations.add("restore-in-place step=%d".formatted(step));
                    assertScanMatches(expected, holder.store, 0, 25, operations);
                } else {
                    SnapshotState snapshot = snapshots.get(random.nextInt(snapshots.size()));
                    Path restoredDir = tempDir.resolve("mixed-store-" + nextStoreId++);
                    holder.replaceWith(restoredDir, snapshot.snapshot());
                    expected = copyState(snapshot.visibleState());
                    operations.add("restore-cross-dir step=%d dir=%s".formatted(step, restoredDir.getFileName()));
                    assertScanMatches(expected, holder.store, 0, 25, operations);
                }
            }

            operations.add("final-scan");
            assertScanMatches(expected, holder.store, 0, 25, operations);
        }
    }

    private static void assertGetMatches(
        NavigableMap<Integer, ModelValue> expected,
        StorageEngine store,
        int key,
        List<String> operations
    ) {
        Optional<ValueRecord> actual = store.get(key(key));
        ModelValue model = expected.get(key);

        if (model == null) {
            assertTrue(actual.isEmpty(), String.join("\n", operations));
            return;
        }

        ValueRecord value = actual.orElseThrow();
        assertEquals(model.value(), value.value(), String.join("\n", operations));
        assertEquals(new Revision(model.revision()), value.modRevision(), String.join("\n", operations));
    }

    private static void assertScanMatches(
        NavigableMap<Integer, ModelValue> expected,
        StorageEngine store,
        int startInclusive,
        int endExclusive,
        List<String> operations
    ) {
        int start = Math.min(startInclusive, endExclusive);
        int end = Math.max(startInclusive, endExclusive);

        List<EntryRecord> actualEntries = new ArrayList<>();
        try (Scan cursor = store.scan(KeyRange.between(key(start), key(end)))) {
            for (EntryRecord entry : cursor) {
                actualEntries.add(entry);
            }
        }

        NavigableMap<Integer, ModelValue> expectedRange = expected.subMap(start, true, end, false);
        assertEquals(
            expectedRange.size(),
            actualEntries.size(),
            "expectedKeys=%s actualKeys=%s%n%s".formatted(
                expectedRange.keySet(),
                actualEntries.stream().map(entry -> Byte.toUnsignedInt(entry.key().byteAt(0))).toList(),
                String.join("\n", operations)
            )
        );

        int index = 0;
        for (var entry : expectedRange.entrySet()) {
            EntryRecord actual = actualEntries.get(index++);
            assertEquals(key(entry.getKey()), actual.key(), String.join("\n", operations));
            assertEquals(entry.getValue().value(), actual.value(), String.join("\n", operations));
            assertEquals(new Revision(entry.getValue().revision()), actual.modRevision(), String.join("\n", operations));
        }
    }

    private static Bytes key(int key) {
        return Bytes.copyOf(new byte[]{(byte) key});
    }

    private static Bytes valueFor(int step, int key) {
        return Bytes.copyOf(new byte[]{(byte) step, (byte) key, (byte) (step ^ key)});
    }

    private static NavigableMap<Integer, ModelValue> copyState(NavigableMap<Integer, ModelValue> source) {
        NavigableMap<Integer, ModelValue> copy = new TreeMap<>();
        source.forEach(copy::put);
        return copy;
    }

    private record ModelValue(Bytes value, long revision) {}

    private record SnapshotState(StorageCheckpoint snapshot, NavigableMap<Integer, ModelValue> visibleState) {}

    private static final class StoreHolder implements AutoCloseable {
        private final StorageOptions options;
        private StorageEngine store;
        private Path directory;

        private StoreHolder(Path directory, StorageOptions options, StorageEngine store) {
            this.directory = directory;
            this.options = options;
            this.store = store;
        }

        static StoreHolder open(Path directory, StorageOptions options) {
            return new StoreHolder(directory, options, StorageEngine.open(directory, options));
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

        void restoreInPlace(StorageCheckpoint checkpoint) {
            store.restoreInPlace(checkpoint);
        }

        @Override
        public void close() {
            store.close();
        }
    }
}
