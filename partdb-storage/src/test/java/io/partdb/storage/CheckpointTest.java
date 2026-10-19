package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static io.partdb.storage.Fixtures.checkpoint;
import static io.partdb.storage.Fixtures.delete;
import static io.partdb.storage.Fixtures.entries;
import static io.partdb.storage.Fixtures.put;
import static io.partdb.storage.Fixtures.restore;
import static io.partdb.storage.Fixtures.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointTest {

    @TempDir
    Path directory;

    @Test
    void checkpointReturnsTheAppliedRevisionIncludingTrailingEmptyApplies() {
        try (Store store = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "one")));
            store.apply(2, List.of());
            store.apply(3, List.of());

            var out = new ByteArrayOutputStream();
            assertEquals(3, store.checkpoint(out));
            assertEquals(3, store.appliedRevision());
            assertEquals(1, store.stats().checkpointCount());
        }
    }

    @Test
    void restoreRoundTripsUnflushedAndFlushedState() {
        byte[] image;
        List<KeyValue> expected;
        long index;
        try (Store source = Store.open(directory.resolve("source"), Fixtures.SMALL)) {
            index = Fixtures.fill(source, 0, 1_000, "v");
            source.apply(++index, List.of(delete(Fixtures.key(10)), put("z", "last")));
            source.apply(++index, List.of());
            Fixtures.await("some tables", () -> source.stats().tableCount() > 0);
            image = checkpoint(source);
            expected = entries(source);
        }

        try (Store target = Store.open(directory.resolve("target"), Fixtures.SMALL)) {
            assertEquals(index, restore(target, image));
            assertEquals(index, target.appliedRevision());
            assertEquals(index, target.durableRevision());
            assertEquals(expected, entries(target));
            assertNull(value(target, Fixtures.key(10)));
            assertEquals(1, target.stats().restoreCount());
        }

        try (Store reopened = Store.open(directory.resolve("target"), Fixtures.SMALL)) {
            assertEquals(index, reopened.appliedRevision());
            assertEquals(index, reopened.durableRevision());
            assertEquals(expected, entries(reopened));
        }
    }

    @Test
    void restoreReplacesExistingStateAndAcceptsLaterApplies() {
        byte[] image;
        try (Store source = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            source.apply(5, List.of(put("a", "from-checkpoint")));
            image = checkpoint(source);
        }

        Path targetDirectory = directory.resolve("target");
        try (Store target = Store.open(targetDirectory, Fixtures.SMALL)) {
            long index = Fixtures.fill(target, 0, 500, "stale-");
            assertTrue(index > 5);

            assertEquals(5, restore(target, image));
            assertEquals(5, target.appliedRevision());
            assertEquals(5, target.durableRevision());
            assertEquals("from-checkpoint", value(target, "a"));
            assertNull(value(target, Fixtures.key(1)));
            assertEquals(1, entries(target).size());

            target.apply(6, List.of(put("b", "after")));
            assertEquals("after", value(target, "b"));
        }
        List<String> tables = Fixtures.tableFiles(targetDirectory);

        try (Store reopened = Store.open(targetDirectory, Fixtures.SMALL)) {
            assertEquals(tables, Fixtures.tableFiles(targetDirectory), "restore must not leave unreferenced tables behind");
            assertEquals(6, reopened.appliedRevision());
            assertEquals("from-checkpoint", value(reopened, "a"));
            assertEquals("after", value(reopened, "b"));
        }
    }

    @Test
    void snapshotsOpenedBeforeRestoreKeepReadingTheOldState() {
        byte[] image;
        try (Store source = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            source.apply(1, List.of(put("a", "new")));
            image = checkpoint(source);
        }
        try (Store target = Store.open(directory.resolve("target"), Fixtures.SMALL)) {
            long index = Fixtures.fill(target, 0, 500, "old-");
            target.apply(++index, List.of(put("a", "old")));
            try (Snapshot before = target.snapshot()) {
                restore(target, image);

                assertEquals("old", value(before, "a"));
                assertEquals("old-7", value(before, Fixtures.key(7)));
                assertEquals(501, Fixtures.keys(before, KeyRange.all()).size());
                assertEquals("new", value(target, "a"));
            }
        }
    }

    @Test
    void emptyStoreCheckpointRestoresToRevisionZero() {
        byte[] image;
        try (Store source = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            image = checkpoint(source);
        }
        try (Store target = Store.open(directory.resolve("target"), StoreOptions.defaults())) {
            target.apply(1, List.of(put("a", "1")));
            assertEquals(0, restore(target, image));
            assertEquals(0, target.appliedRevision());
            assertNull(value(target, "a"));
        }
    }

    @Test
    void failedRestoreLeavesTheStoreUntouched() {
        byte[] image;
        try (Store source = Store.open(directory.resolve("source"), Fixtures.SMALL)) {
            Fixtures.fill(source, 0, 300, "src-");
            image = checkpoint(source);
        }
        byte[] truncated = Arrays.copyOf(image, image.length - 10);
        byte[] flipped = image.clone();
        flipped[flipped.length / 2] ^= 0x40;

        Path targetDirectory = directory.resolve("target");
        try (Store target = Store.open(targetDirectory, StoreOptions.defaults())) {
            target.apply(1, List.of(put("a", "kept")));
            List<String> files = Fixtures.fileNames(targetDirectory);

            assertThrows(StoreException.Corruption.class, () -> restore(target, truncated));
            assertThrows(StoreException.Corruption.class, () -> restore(target, flipped));
            assertEquals(files, Fixtures.fileNames(targetDirectory), "failed restores must remove their staged tables");

            assertEquals(1, target.appliedRevision());
            assertEquals("kept", value(target, "a"));
            target.apply(2, List.of(put("b", "also")));
        }
        try (Store reopened = Store.open(targetDirectory, StoreOptions.defaults())) {
            assertEquals("kept", value(reopened, "a"));
            assertEquals("also", value(reopened, "b"));
        }
    }
}
