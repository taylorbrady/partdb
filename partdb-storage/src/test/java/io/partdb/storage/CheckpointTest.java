package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static io.partdb.storage.Fixtures.bytes;
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
    void checkpointReturnsTheAppliedIndexIncludingTrailingEmptyApplies() {
        try (Store store = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "one")));
            store.apply(2, List.of());
            store.apply(3, List.of());

            var out = new ByteArrayOutputStream();
            assertEquals(3, store.checkpoint(out));
            assertEquals(3, store.appliedIndex());
            assertEquals(1, store.stats().checkpointCount());
        }
    }

    @Test
    void restoreRoundTripsUnflushedAndFlushedState() {
        byte[] image;
        List<Entry> expected;
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
            assertEquals(index, target.appliedIndex());
            assertEquals(index, target.durableIndex());
            assertEquals(expected, entries(target));
            assertNull(value(target, Fixtures.key(10)));
            assertEquals(1, target.stats().restoreCount());
        }

        try (Store reopened = Store.open(directory.resolve("target"), Fixtures.SMALL)) {
            assertEquals(index, reopened.appliedIndex());
            assertEquals(index, reopened.durableIndex());
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
            assertEquals(5, target.appliedIndex());
            assertEquals(5, target.durableIndex());
            assertEquals("from-checkpoint", value(target, "a"));
            assertNull(value(target, Fixtures.key(1)));
            assertEquals(1, entries(target).size());

            target.apply(6, List.of(put("b", "after")));
            assertEquals("after", value(target, "b"));
        }
        assertEquals(1, Fixtures.fileNames(targetDirectory).stream().filter(name -> name.startsWith("gen-")).count());

        try (Store reopened = Store.open(targetDirectory, Fixtures.SMALL)) {
            assertEquals(6, reopened.appliedIndex());
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
    void emptyStoreCheckpointRestoresToIndexZero() {
        byte[] image;
        try (Store source = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            image = checkpoint(source);
        }
        try (Store target = Store.open(directory.resolve("target"), StoreOptions.defaults())) {
            target.apply(1, List.of(put("a", "1")));
            assertEquals(0, restore(target, image));
            assertEquals(0, target.appliedIndex());
            assertTrue(target.get(bytes("a")).isEmpty());
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

            assertThrows(StoreException.Corruption.class, () -> restore(target, truncated));
            assertThrows(StoreException.Corruption.class, () -> restore(target, flipped));

            assertEquals(1, target.appliedIndex());
            assertEquals("kept", value(target, "a"));
            target.apply(2, List.of(put("b", "also")));
        }
        assertEquals(1, Fixtures.fileNames(targetDirectory).stream().filter(name -> name.startsWith("gen-")).count());
        try (Store reopened = Store.open(targetDirectory, StoreOptions.defaults())) {
            assertEquals("kept", value(reopened, "a"));
            assertEquals("also", value(reopened, "b"));
        }
    }
}
