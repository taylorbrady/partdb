package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
