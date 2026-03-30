package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void putGetAndDeleteUseByteBasedApi() {
        try (StateStore store = StateStore.open(tempDir, StorageConfig.defaults())) {
            byte[] key = bytes("key");
            byte[] value = bytes("value");

            store.put(key, value, 10);

            Optional<VersionedEntry> loaded = store.get(key);
            assertTrue(loaded.isPresent());
            assertArrayEquals(key, loaded.get().key());
            assertArrayEquals(value, loaded.get().value());
            assertEquals(10, loaded.get().revision());

            store.delete(key, 11);

            assertTrue(store.get(key).isEmpty());
        }
    }

    @Test
    void scanExposesExplicitCursorLifecycle() {
        try (StateStore store = StateStore.open(tempDir, StorageConfig.defaults())) {
            store.put(bytes("a"), bytes("1"), 1);
            store.put(bytes("b"), bytes("2"), 2);
            store.put(bytes("c"), bytes("3"), 3);

            List<String> keys = new ArrayList<>();
            try (StorageCursor cursor = store.scan(bytes("a"), bytes("c"))) {
                while (cursor.hasNext()) {
                    keys.add(new String(cursor.next().key(), StandardCharsets.UTF_8));
                }
            }

            assertEquals(List.of("a", "b"), keys);
        }
    }

    @Test
    void snapshotRestoresIntoFreshDirectory() {
        Path sourceDir = tempDir.resolve("source");
        Path restoredDir = tempDir.resolve("restored");

        StorageSnapshot snapshot;
        try (StateStore source = StateStore.open(sourceDir, StorageConfig.defaults())) {
            source.put(bytes("key-1"), bytes("value-1"), 1);
            source.put(bytes("key-2"), bytes("value-2"), 2);
            snapshot = source.snapshot();
        }

        try (StateStore restored = StateStore.open(restoredDir, StorageConfig.defaults())) {
            restored.restore(snapshot);

            Optional<VersionedEntry> value1 = restored.get(bytes("key-1"));
            Optional<VersionedEntry> value2 = restored.get(bytes("key-2"));

            assertTrue(value1.isPresent());
            assertTrue(value2.isPresent());
            assertArrayEquals(bytes("value-1"), value1.get().value());
            assertArrayEquals(bytes("value-2"), value2.get().value());
        }
    }

    @Test
    void rejectsStaleRevisionForExistingValue() {
        try (StateStore store = StateStore.open(tempDir, StorageConfig.defaults())) {
            byte[] key = bytes("key");
            store.put(key, bytes("value-1"), 10);

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> store.put(key, bytes("value-2"), 9)
            );

            assertTrue(error.getMessage().contains("older"));
            assertArrayEquals(bytes("value-1"), store.get(key).orElseThrow().value());
        }
    }

    @Test
    void allowsIdempotentReplayAtSameRevision() {
        try (StateStore store = StateStore.open(tempDir, StorageConfig.defaults())) {
            byte[] key = bytes("key");
            byte[] value = bytes("value");

            store.put(key, value, 10);
            store.put(key, value, 10);

            Optional<VersionedEntry> loaded = store.get(key);
            assertTrue(loaded.isPresent());
            assertArrayEquals(value, loaded.get().value());
            assertEquals(10, loaded.get().revision());
        }
    }

    @Test
    void rejectsStaleRevisionAgainstPersistedTombstone() {
        try (StateStore store = StateStore.open(tempDir, StorageConfig.defaults())) {
            byte[] key = bytes("key");
            store.put(key, bytes("value"), 10);
            store.delete(key, 11);
            store.snapshot();

            StorageException.InvalidRevision error = assertThrows(
                StorageException.InvalidRevision.class,
                () -> store.put(key, bytes("late-value"), 10)
            );

            assertTrue(error.getMessage().contains("older"));
            assertTrue(store.get(key).isEmpty());
        }
    }

    @Test
    void openFailsWhenManifestIsMissingButSstablesRemain() throws Exception {
        Path storeDir = tempDir.resolve("store");

        try (StateStore store = StateStore.open(storeDir, StorageConfig.defaults())) {
            store.put(bytes("key"), bytes("value"), 1);
            store.snapshot();
        }

        Files.delete(storeDir.resolve("MANIFEST"));

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> StateStore.open(storeDir, StorageConfig.defaults())
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
            () -> StateStore.open(storeDir, StorageConfig.defaults())
        );

        assertTrue(error.getMessage().contains("manifest"));
    }

    @Test
    void openFailsWithTruncatedSstableAsCorruption() throws Exception {
        Path storeDir = tempDir.resolve("truncated-sstable");

        try (StateStore store = StateStore.open(storeDir, StorageConfig.defaults())) {
            store.put(bytes("key"), bytes("value"), 1);
            store.snapshot();
        }

        Path sstable = firstSstable(storeDir);
        byte[] bytes = Files.readAllBytes(sstable);
        Files.write(sstable, Arrays.copyOf(bytes, SSTableHeader.HEADER_SIZE));

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> StateStore.open(storeDir, StorageConfig.defaults())
        );

        assertTrue(error.getMessage().contains("SSTable"));
    }

    @Test
    void getFailsWithCorruptedSstableBlockAsCorruption() throws Exception {
        Path storeDir = tempDir.resolve("corrupted-block");

        try (StateStore store = StateStore.open(storeDir, StorageConfig.defaults())) {
            store.put(bytes("key"), bytes("value"), 1);
            store.snapshot();
        }

        Path sstable = firstSstable(storeDir);
        byte[] fileBytes = Files.readAllBytes(sstable);
        fileBytes[SSTableHeader.HEADER_SIZE] ^= 0x01;
        Files.write(sstable, fileBytes);

        try (StateStore reopened = StateStore.open(storeDir, StorageConfig.defaults())) {
            StorageException.Corruption error = assertThrows(
                StorageException.Corruption.class,
                () -> reopened.get(bytes("key"))
            );

            assertTrue(error.getMessage().contains("Block") || error.getMessage().contains("CompressedBlock"));
        }
    }

    @Test
    void restoreRejectsCorruptedSnapshotWithoutDiscardingLiveState() {
        try (StateStore store = StateStore.open(tempDir, StorageConfig.defaults())) {
            byte[] key = bytes("key");
            store.put(key, bytes("before"), 1);
            StorageSnapshot snapshot = store.snapshot();

            store.put(key, bytes("after"), 2);

            StorageException.Corruption error = assertThrows(
                StorageException.Corruption.class,
                () -> store.restore(new StorageSnapshot(corruptFirstSstableByte(snapshot)))
            );

            assertTrue(error.getMessage().contains("Checkpoint"));
            Optional<VersionedEntry> loaded = store.get(key);
            assertTrue(loaded.isPresent());
            assertArrayEquals(bytes("after"), loaded.get().value());
            assertEquals(2, loaded.get().revision());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] corruptFirstSstableByte(StorageSnapshot snapshot) {
        byte[] bytes = snapshot.bytes();
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
        return bytes;
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
