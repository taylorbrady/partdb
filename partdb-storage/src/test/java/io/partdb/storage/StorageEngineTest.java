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

        try (StorageEngine restored = StorageEngine.restore(restoredDir, checkpoint, StorageOptions.defaults())) {

            Optional<ValueRecord> value1 = restored.get(bytes("key-1"));
            Optional<ValueRecord> value2 = restored.get(bytes("key-2"));

            assertTrue(value1.isPresent());
            assertTrue(value2.isPresent());
            assertEquals(bytes("value-1"), value1.get().value());
            assertEquals(bytes("value-2"), value2.get().value());
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
                () -> store.restoreInPlace(corruptFirstSstableByte(checkpoint))
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

        try (StorageEngine restored = StorageEngine.restore(restoredDir, checkpoint, StorageOptions.defaults())) {
            assertEquals(new Revision(9), restored.metadata().appliedThrough());
        }
    }

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
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
