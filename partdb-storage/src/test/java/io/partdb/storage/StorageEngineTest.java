package io.partdb.storage;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineTest {

    @TempDir
    Path tempDir;

    @Test
    void putGetAndDeleteUseBytesBasedApi() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            Bytes key = bytes("key");
            Bytes value = bytes("value");

            store.apply(new Revision(10), Mutation.put(key, value));

            Optional<ValueRecord> loaded = store.get(key);
            assertTrue(loaded.isPresent());
            assertEquals(value, loaded.get().value());
            assertEquals(new Revision(10), loaded.get().modRevision());

            store.apply(new Revision(11), Mutation.delete(key));

            assertTrue(store.get(key).isEmpty());
        }
    }

    @Test
    void scanExposesExplicitCursorLifecycle() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            store.apply(new Revision(1), Mutation.put(bytes("a"), bytes("1")));
            store.apply(new Revision(2), Mutation.put(bytes("b"), bytes("2")));
            store.apply(new Revision(3), Mutation.put(bytes("c"), bytes("3")));

            List<String> keys = new ArrayList<>();
            try (Scan cursor = store.scan(KeyRange.between(bytes("a"), bytes("c")))) {
                for (EntryRecord entry : cursor) {
                    keys.add(entry.key().utf8());
                }
            }

            assertEquals(List.of("a", "b"), keys);
        }
    }

    @Test
    void snapshotRestoresIntoFreshDirectory() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        StorageCheckpoint checkpoint;
        try (StorageEngine source = StorageEngine.open(sourceDir, StorageOptions.defaults())) {
            source.apply(new Revision(1), Mutation.put(bytes("key-1"), bytes("value-1")));
            source.apply(new Revision(2), Mutation.put(bytes("key-2"), bytes("value-2")));
            checkpoint = source.checkpoint();
        }

        try (StorageEngine restored = StorageEngine.openFromCheckpoint(restoredDir, checkpoint, StorageOptions.defaults())) {

            Optional<ValueRecord> value1 = restored.get(bytes("key-1"));
            Optional<ValueRecord> value2 = restored.get(bytes("key-2"));

            assertTrue(value1.isPresent());
            assertTrue(value2.isPresent());
            assertEquals(bytes("value-1"), value1.get().value());
            assertEquals(bytes("value-2"), value2.get().value());
        }
    }

    @Test
    void restoreInPlaceReplacesReusedSstableIdsForGetAndScan() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            StorageCheckpoint empty = store.checkpoint();

            store.apply(new Revision(1), Mutation.put(bytes("b"), bytes("before")));
            StorageCheckpoint before = store.checkpoint();

            store.restore(empty);
            store.apply(new Revision(2), Mutation.put(bytes("a"), bytes("after")));
            store.checkpoint();

            store.restore(before);

            Optional<ValueRecord> restored = store.get(bytes("b"));
            assertTrue(restored.isPresent());
            assertEquals(bytes("before"), restored.get().value());
            assertEquals(new Revision(1), restored.get().modRevision());
            assertTrue(store.get(bytes("a")).isEmpty());

            List<EntryRecord> entries = new ArrayList<>();
            try (Scan scan = store.scan(KeyRange.all())) {
                for (EntryRecord entry : scan) {
                    entries.add(entry);
                }
            }

            assertEquals(1, entries.size());
            assertEquals(bytes("b"), entries.getFirst().key());
            assertEquals(bytes("before"), entries.getFirst().value());
            assertEquals(new Revision(1), entries.getFirst().modRevision());
        }
    }

    @Test
    void restoreInPlaceSurfacesCheckpointDataAfterCrossDirectoryHistory() {
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
        StorageCheckpoint key21;

        try (StorageEngine store0 = StorageEngine.open(dir0, options)) {
            empty = store0.checkpoint();
            store0.apply(new Revision(1), Mutation.put(bytesFromInt(7), bytes("v1")));
            store0.restore(empty);
            store0.restore(empty);

            store0.apply(new Revision(2), Mutation.put(bytesFromInt(23), bytes("v23")));
            key23 = store0.checkpoint();

            store0.apply(new Revision(3), Mutation.put(bytesFromInt(15), bytes("v15")));
            store0.apply(new Revision(4), Mutation.put(bytesFromInt(10), bytes("v10")));
            store0.apply(new Revision(5), Mutation.put(bytesFromInt(14), bytes("v14")));
        }

        try (StorageEngine store1 = StorageEngine.openFromCheckpoint(dir1, key23, options)) {
            store1.apply(new Revision(6), Mutation.delete(bytesFromInt(20)));
        }

        try (StorageEngine store2 = StorageEngine.openFromCheckpoint(dir2, empty, options)) {
            store2.apply(new Revision(7), Mutation.put(bytesFromInt(21), bytes("v21")));
            key21 = store2.checkpoint();

            store2.apply(new Revision(8), Mutation.put(bytesFromInt(13), bytes("v13")));
            store2.apply(new Revision(9), Mutation.put(bytesFromInt(20), bytes("v20-r9")));
            store2.apply(new Revision(10), Mutation.put(bytesFromInt(23), bytes("v23-r10")));
            store2.apply(new Revision(11), Mutation.put(bytesFromInt(8), bytes("v8")));
            store2.apply(new Revision(12), Mutation.put(bytesFromInt(2), bytes("v2")));
            store2.apply(new Revision(13), Mutation.put(bytesFromInt(20), bytes("v20-r13")));
            store2.apply(new Revision(14), Mutation.delete(bytesFromInt(15)));
            store2.apply(new Revision(15), Mutation.put(bytesFromInt(12), bytes("v12")));

            store2.restore(key23);

            Optional<ValueRecord> restored = store2.get(bytesFromInt(23));
            assertTrue(restored.isPresent());
            assertEquals(bytes("v23"), restored.get().value());
            assertEquals(new Revision(2), restored.get().modRevision());
            assertTrue(store2.get(bytesFromInt(21)).isEmpty());

            List<EntryRecord> entries = new ArrayList<>();
            try (Scan scan = store2.scan(KeyRange.between(bytesFromInt(0), bytesFromInt(25)))) {
                for (EntryRecord entry : scan) {
                    entries.add(entry);
                }
            }

            assertEquals(1, entries.size());
            assertEquals(bytesFromInt(23), entries.getFirst().key());
            assertEquals(bytes("v23"), entries.getFirst().value());
            assertEquals(new Revision(2), entries.getFirst().modRevision());

            store2.restore(key21);
            assertTrue(store2.get(bytesFromInt(21)).isPresent());
            assertTrue(store2.get(bytesFromInt(23)).isEmpty());
        }
    }

    @Test
    void rejectsStaleRevisionForExistingValue() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            Bytes key = bytes("key");
            store.apply(new Revision(10), Mutation.put(key, bytes("value-1")));

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> store.apply(new Revision(9), Mutation.put(key, bytes("value-2")))
            );

            assertTrue(error.getMessage().contains("older"));
            assertEquals(bytes("value-1"), store.get(key).orElseThrow().value());
        }
    }

    @Test
    void allowsIdempotentReplayAtSameRevision() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            Bytes key = bytes("key");
            Bytes value = bytes("value");

            store.apply(new Revision(10), Mutation.put(key, value));
            store.apply(new Revision(10), Mutation.put(key, value));

            Optional<ValueRecord> loaded = store.get(key);
            assertTrue(loaded.isPresent());
            assertEquals(value, loaded.get().value());
            assertEquals(new Revision(10), loaded.get().modRevision());
        }
    }

    @Test
    void rejectsStaleRevisionAgainstPersistedTombstone() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            Bytes key = bytes("key");
            store.apply(new Revision(10), Mutation.put(key, bytes("value")));
            store.apply(new Revision(11), Mutation.delete(key));
            store.checkpoint();

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> store.apply(new Revision(10), Mutation.put(key, bytes("late-value")))
            );

            assertTrue(error.getMessage().contains("older"));
            assertTrue(store.get(key).isEmpty());
        }
    }

    @Test
    void openFailsWhenManifestIsMissingButSstablesRemain() throws Exception {
        Path storeDir = tempDir.resolve("store");

        try (StorageEngine store = StorageEngine.open(storeDir, StorageOptions.defaults())) {
            store.apply(new Revision(1), Mutation.put(bytes("key"), bytes("value")));
            store.checkpoint();
        }

        Files.delete(storeDir.resolve("MANIFEST"));

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> StorageEngine.open(storeDir, StorageOptions.defaults())
        );

        assertTrue(error.getMessage().contains("manifest"));
    }

    @Test
    void openFailsWithMalformedManifestAsCorruption() throws Exception {
        Path storeDir = tempDir.resolve("malformed-manifest");
        Files.createDirectories(storeDir);
        Files.write(storeDir.resolve("MANIFEST"), new byte[]{0x01, 0x02, 0x03});

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> StorageEngine.open(storeDir, StorageOptions.defaults())
        );

        assertTrue(error.getMessage().contains("manifest"));
    }

    @Test
    void openFailsWithTruncatedSstableAsCorruption() throws Exception {
        Path storeDir = tempDir.resolve("truncated-sstable");

        try (StorageEngine store = StorageEngine.open(storeDir, StorageOptions.defaults())) {
            store.apply(new Revision(1), Mutation.put(bytes("key"), bytes("value")));
            store.checkpoint();
        }

        Path sstable = firstSstable(storeDir);
        byte[] bytes = Files.readAllBytes(sstable);
        Files.write(sstable, Arrays.copyOf(bytes, SSTableHeader.HEADER_SIZE));

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> StorageEngine.open(storeDir, StorageOptions.defaults())
        );

        assertTrue(error.getMessage().contains("SSTable"));
    }

    @Test
    void getFailsWithCorruptedSstableBlockAsCorruption() throws Exception {
        Path storeDir = tempDir.resolve("corrupted-block");

        try (StorageEngine store = StorageEngine.open(storeDir, StorageOptions.defaults())) {
            store.apply(new Revision(1), Mutation.put(bytes("key"), bytes("value")));
            store.checkpoint();
        }

        Path sstable = firstSstable(storeDir);
        byte[] fileBytes = Files.readAllBytes(sstable);
        fileBytes[SSTableHeader.HEADER_SIZE] ^= 0x01;
        Files.write(sstable, fileBytes);

        try (StorageEngine reopened = StorageEngine.open(storeDir, StorageOptions.defaults())) {
            StorageException.Corruption error = assertThrows(
                StorageException.Corruption.class,
                () -> reopened.get(bytes("key"))
            );

            assertTrue(error.getMessage().contains("Block") || error.getMessage().contains("CompressedBlock"));
        }
    }

    @Test
    void restoreRejectsCorruptedSnapshotWithoutDiscardingLiveState() {
        try (StorageEngine store = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            Bytes key = bytes("key");
            store.apply(new Revision(1), Mutation.put(key, bytes("before")));
            StorageCheckpoint checkpoint = store.checkpoint();

            store.apply(new Revision(2), Mutation.put(key, bytes("after")));

            StorageException.Corruption error = assertThrows(
                StorageException.Corruption.class,
                () -> store.restore(corruptFirstSstableByte(checkpoint))
            );

            assertTrue(error.getMessage().contains("Checkpoint"));
            Optional<ValueRecord> loaded = store.get(key);
            assertTrue(loaded.isPresent());
            assertEquals(bytes("after"), loaded.get().value());
            assertEquals(new Revision(2), loaded.get().modRevision());
        }
    }

    @Test
    void metadataTracksAppliedRevisionAcrossReopenAndRestore() {
        Path storeDir = tempDir.resolve("metadata");
        Path restoredDir = tempDir.resolve("metadata-restored");

        StorageCheckpoint checkpoint;
        try (StorageEngine store = StorageEngine.open(storeDir, StorageOptions.defaults())) {
            store.apply(new Revision(4), Mutation.put(bytes("key"), bytes("value")));
            store.apply(new Revision(9), Mutation.delete(bytes("key")));

            assertEquals(new Revision(9), store.metadata().appliedThrough());
            checkpoint = store.checkpoint();
        }

        try (StorageEngine reopened = StorageEngine.open(storeDir, StorageOptions.defaults())) {
            assertEquals(new Revision(9), reopened.metadata().appliedThrough());
        }

        try (StorageEngine restored = StorageEngine.openFromCheckpoint(restoredDir, checkpoint, StorageOptions.defaults())) {
            assertEquals(new Revision(9), restored.metadata().appliedThrough());
        }
    }

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    private static Bytes bytesFromInt(int value) {
        return Bytes.copyOf(new byte[]{(byte) value});
    }

    private static StorageCheckpoint corruptFirstSstableByte(StorageCheckpoint checkpoint) {
        byte[] bytes = checkpoint.bytes().toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.getInt();
        buffer.getInt();
        int manifestLength = buffer.getInt();
        int sstableCount = buffer.getInt();
        if (sstableCount <= 0) {
            throw new IllegalArgumentException("snapshot does not contain any SSTables");
        }

        int payloadOffset = Integer.BYTES * 4 + manifestLength + Long.BYTES + Integer.BYTES;
        if (payloadOffset >= bytes.length) {
            throw new IllegalArgumentException("snapshot payload offset out of range");
        }

        bytes[payloadOffset] ^= 0x01;
        return new StorageCheckpoint(Bytes.copyOf(bytes));
    }

    private static Path firstSstable(Path directory) throws Exception {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                .filter(path -> path.getFileName().toString().endsWith(".sst"))
                .findFirst()
                .orElseThrow(() -> new NoSuchFileException("No SSTable in " + directory));
        }
    }
}
