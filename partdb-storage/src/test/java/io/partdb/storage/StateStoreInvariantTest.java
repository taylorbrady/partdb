package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateStoreInvariantTest {

    @TempDir
    Path tempDir;

    @Test
    void randomizedOperationsPreserveVisibleStateAcrossReopenAndRestore() {
        StorageConfig config = StorageConfig.builder()
            .writeBufferMaxBytes(256)
            .tuning(StorageConfig.Tuning.builder()
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

        try (StateStoreHolder holder = StateStoreHolder.open(currentDir, config)) {
            for (int step = 0; step < 300; step++) {
                int op = random.nextInt(100);
                if (op < 45) {
                    int key = random.nextInt(24);
                    byte[] value = valueFor(step, key);
                    revision++;
                    holder.store.put(key(key), value, revision);
                    expected.put(key, new ModelValue(value, revision));
                    operations.add("put step=%d key=%d rev=%d".formatted(step, key, revision));
                } else if (op < 65) {
                    int key = random.nextInt(24);
                    revision++;
                    holder.store.delete(key(key), revision);
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
                    StorageSnapshot snapshot = holder.store.snapshot();
                    Path restoredDir = tempDir.resolve("store-" + nextStoreId++);
                    holder.replaceWith(restoredDir, snapshot);
                    operations.add("restore step=%d dir=%s".formatted(step, restoredDir.getFileName()));
                }
            }

            operations.add("final-scan");
            assertScanMatches(expected, holder.store, 0, 25, operations);
        }
    }

    private static void assertGetMatches(
        NavigableMap<Integer, ModelValue> expected,
        StateStore store,
        int key,
        List<String> operations
    ) {
        Optional<VersionedEntry> actual = store.get(key(key));
        ModelValue model = expected.get(key);

        if (model == null) {
            assertTrue(actual.isEmpty(), String.join("\n", operations));
            return;
        }

        VersionedEntry entry = actual.orElseThrow();
        assertArrayEquals(key(key), entry.key(), String.join("\n", operations));
        assertArrayEquals(model.value(), entry.value(), String.join("\n", operations));
        assertEquals(model.revision(), entry.revision(), String.join("\n", operations));
    }

    private static void assertScanMatches(
        NavigableMap<Integer, ModelValue> expected,
        StateStore store,
        int startInclusive,
        int endExclusive,
        List<String> operations
    ) {
        int start = Math.min(startInclusive, endExclusive);
        int end = Math.max(startInclusive, endExclusive);

        List<VersionedEntry> actualEntries = new ArrayList<>();
        try (StorageCursor cursor = store.scan(key(start), key(end))) {
            while (cursor.hasNext()) {
                actualEntries.add(cursor.next());
            }
        }

        NavigableMap<Integer, ModelValue> expectedRange = expected.subMap(start, true, end, false);
        assertEquals(
            expectedRange.size(),
            actualEntries.size(),
            "expectedKeys=%s actualKeys=%s%n%s".formatted(
                expectedRange.keySet(),
                actualEntries.stream().map(entry -> Byte.toUnsignedInt(entry.key()[0])).toList(),
                String.join("\n", operations)
            )
        );

        int index = 0;
        for (var entry : expectedRange.entrySet()) {
            VersionedEntry actual = actualEntries.get(index++);
            assertArrayEquals(key(entry.getKey()), actual.key(), String.join("\n", operations));
            assertArrayEquals(entry.getValue().value(), actual.value(), String.join("\n", operations));
            assertEquals(entry.getValue().revision(), actual.revision(), String.join("\n", operations));
        }
    }

    private static byte[] key(int key) {
        return new byte[]{(byte) key};
    }

    private static byte[] valueFor(int step, int key) {
        return new byte[]{(byte) step, (byte) key, (byte) (step ^ key)};
    }

    private record ModelValue(byte[] value, long revision) {}

    private static final class StateStoreHolder implements AutoCloseable {
        private final StorageConfig config;
        private StateStore store;
        private Path directory;

        private StateStoreHolder(Path directory, StorageConfig config, StateStore store) {
            this.directory = directory;
            this.config = config;
            this.store = store;
        }

        static StateStoreHolder open(Path directory, StorageConfig config) {
            return new StateStoreHolder(directory, config, StateStore.open(directory, config));
        }

        void reopen() {
            store.close();
            store = StateStore.open(directory, config);
        }

        void replaceWith(Path directory, StorageSnapshot snapshot) {
            store.close();
            this.directory = directory;
            this.store = StateStore.open(directory, config);
            this.store.restore(snapshot);
        }

        @Override
        public void close() {
            store.close();
        }
    }
}
