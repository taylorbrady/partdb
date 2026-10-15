package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.partdb.storage.Fixtures.fileNames;
import static io.partdb.storage.Fixtures.generation;
import static io.partdb.storage.Fixtures.put;
import static io.partdb.storage.Fixtures.tableFiles;
import static io.partdb.storage.Fixtures.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashRecoveryTest {

    @TempDir
    Path directory;

    @Test
    void openCreatesAGenerationPointedToByCurrent() throws IOException {
        try (Store _ = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(List.of("CURRENT", "gen-1"), fileNames(directory));
            assertEquals("gen-1", Files.readString(directory.resolve("CURRENT")));
            assertEquals(List.of("MANIFEST"), fileNames(directory.resolve("gen-1")));
        }
    }

    @Test
    void orphanTablesAreDeletedAndTheirIdsAreNeverReused() throws IOException {
        seed();
        Path generation = generation(directory);
        List<String> committed = tableFiles(generation);
        Files.write(generation.resolve("000900.sst"), new byte[]{1, 2, 3});
        Files.write(generation.resolve("000003.sst.tmp"), new byte[]{4});

        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            assertEquals(committed, tableFiles(generation));
            assertFalse(fileNames(generation).contains("000003.sst.tmp"));
            assertEquals("v7", value(store, Fixtures.key(7)));

            Fixtures.fill(store, store.appliedIndex(), 400, "w");
        }
        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            List<String> after = tableFiles(generation);
            assertTrue(after.stream().anyMatch(name -> name.compareTo("000900.sst") > 0), after.toString());
            assertTrue(after.stream().noneMatch(name -> name.equals("000900.sst")));
            assertEquals("w7", value(store, Fixtures.key(7)));
        }
    }

    @Test
    void leftoverTemporaryFilesAreRemoved() throws IOException {
        seed();
        Path generation = generation(directory);
        Files.write(generation.resolve("MANIFEST.tmp"), new byte[]{9, 9});
        Files.write(directory.resolve("CURRENT.tmp"), "gen-7".getBytes(StandardCharsets.US_ASCII));
        Files.write(directory.resolve("checkpoint-abc.tmp"), new byte[]{1});

        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            assertEquals(List.of("CURRENT", generation.getFileName().toString()), fileNames(directory));
            assertFalse(fileNames(generation).contains("MANIFEST.tmp"));
            assertEquals("v1", value(store, Fixtures.key(1)));
        }
    }

    @Test
    void halfStagedGenerationIsDiscardedAndTheOldOneReopens() throws IOException {
        long index = seed();
        Path staged = directory.resolve("gen-2");
        Files.createDirectory(staged);
        Files.write(staged.resolve("000001.sst"), new byte[]{1, 2, 3});

        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            assertEquals(index, store.appliedIndex());
            assertFalse(Files.exists(staged));
            assertEquals("v3", value(store, Fixtures.key(3)));
        }
    }

    @Test
    void swappedCurrentOpensTheNewGenerationAndDeletesTheOld() throws IOException {
        byte[] image;
        try (Store source = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            source.apply(42, List.of(put("a", "restored")));
            image = Fixtures.checkpoint(source);
        }
        Path root = directory.resolve("target");
        try (Store target = Store.open(root, Fixtures.NO_COMPACTION)) {
            Fixtures.fill(target, 0, 100, "old");
        }
        Path old = generation(root);
        Path scratch = directory.resolve("scratch");
        try (Store restored = Store.open(scratch, StoreOptions.defaults())) {
            Fixtures.restore(restored, image);
        }
        Files.move(generation(scratch), root.resolve("gen-5"));
        Files.writeString(root.resolve("CURRENT"), "gen-5");

        try (Store store = Store.open(root, StoreOptions.defaults())) {
            assertEquals(42, store.appliedIndex());
            assertEquals("restored", value(store, "a"));
            assertFalse(Files.exists(old));
        }
    }

    @Test
    void manifestReferencingAMissingTableIsCorruption() throws IOException {
        seed();
        Path generation = generation(directory);
        Files.delete(generation.resolve(tableFiles(generation).getFirst()));

        assertThrows(StoreException.Corruption.class, () -> Store.open(directory, Fixtures.NO_COMPACTION));
    }

    @Test
    void currentPointingAtAMissingGenerationIsCorruption() throws IOException {
        seed();
        Files.writeString(directory.resolve("CURRENT"), "gen-9");

        assertThrows(StoreException.Corruption.class, () -> Store.open(directory, Fixtures.NO_COMPACTION));
    }

    @Test
    void malformedCurrentIsCorruption() throws IOException {
        seed();
        Files.writeString(directory.resolve("CURRENT"), "elsewhere");

        assertThrows(StoreException.Corruption.class, () -> Store.open(directory, Fixtures.NO_COMPACTION));
    }

    private long seed() {
        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            long index = Fixtures.fill(store, 0, 400, "v");
            Fixtures.await("tables", () -> store.stats().tableCount() > 1);
            return index;
        }
    }
}
