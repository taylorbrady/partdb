package io.partdb.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.partdb.storage.Fixtures.fileNames;
import static io.partdb.storage.Fixtures.tableFiles;
import static io.partdb.storage.Fixtures.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashRecoveryTest {

    @TempDir
    Path directory;

    private Path root;

    @BeforeEach
    void setUp() {
        root = directory.resolve("store");
    }

    @Test
    void openCreatesAnEmptyManifestAndALock() {
        try (Store _ = Store.open(root, StoreOptions.defaults())) {
            assertEquals(List.of("LOCK", Manifest.FILE_NAME), fileNames(root));
        }
    }

    @Test
    void orphanTablesAreDeletedAndTheirIdsAreNeverReused() throws IOException {
        seed();
        List<String> committed = tableFiles(root);
        Files.write(root.resolve("000900.sst"), new byte[]{1, 2, 3});

        try (Store store = Store.open(root, Fixtures.NO_COMPACTION)) {
            assertEquals(committed, tableFiles(root));
            assertEquals("v7", value(store, Fixtures.key(7)));

            Fixtures.fill(store, store.appliedRevision(), 400, "w");
        }
        try (Store store = Store.open(root, Fixtures.NO_COMPACTION)) {
            List<String> after = tableFiles(root);
            assertTrue(after.stream().anyMatch(name -> name.compareTo("000900.sst") > 0), after.toString());
            assertFalse(after.contains("000900.sst"));
            assertEquals("w7", value(store, Fixtures.key(7)));
        }
    }

    @Test
    void leftoverTemporaryFilesAreRemoved() throws IOException {
        seed();
        List<String> before = fileNames(root);
        Files.write(root.resolve("MANIFEST.tmp"), new byte[]{9, 9});
        Files.write(root.resolve("000003.sst.tmp"), new byte[]{4});
        Files.write(root.resolve("checkpoint-abc.tmp"), new byte[]{1});

        try (Store store = Store.open(root, Fixtures.NO_COMPACTION)) {
            assertEquals(before, fileNames(root));
            assertEquals("v1", value(store, Fixtures.key(1)));
        }
    }

    @Test
    void tablesStagedByAnInterruptedRestoreAreDiscarded() throws IOException {
        long revision = seed();
        List<String> committed = tableFiles(root);
        try (Store store = Store.open(directory.resolve("source"), Fixtures.NO_COMPACTION)) {
            Fixtures.fill(store, 0, 400, "staged");
            Fixtures.await("tables", () -> store.stats().tableCount() > 0);
        }
        Path source = directory.resolve("source");
        for (String table : tableFiles(source)) {
            Files.copy(source.resolve(table), root.resolve("%06d.sst".formatted(5_000 + Integer.parseInt(table.substring(0, 6)))));
        }

        try (Store store = Store.open(root, Fixtures.NO_COMPACTION)) {
            assertEquals(revision, store.appliedRevision());
            assertEquals(committed, tableFiles(root));
            assertEquals("v3", value(store, Fixtures.key(3)));
        }
    }

    @Test
    void manifestReferencingAMissingTableIsCorruption() throws IOException {
        seed();
        Files.delete(root.resolve(tableFiles(root).getFirst()));

        assertThrows(StoreException.Corruption.class, () -> Store.open(root, Fixtures.NO_COMPACTION));
    }

    @Test
    void tablesWithoutAManifestAreCorruption() throws IOException {
        seed();
        Files.delete(root.resolve(Manifest.FILE_NAME));

        assertThrows(StoreException.Corruption.class, () -> Store.open(root, Fixtures.NO_COMPACTION));
    }

    private long seed() {
        try (Store store = Store.open(root, Fixtures.NO_COMPACTION)) {
            long revision = Fixtures.fill(store, 0, 400, "v");
            Fixtures.await("tables", () -> store.stats().tableCount() > 1);
            return revision;
        }
    }
}
