package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.partdb.storage.Fixtures.bytes;
import static io.partdb.storage.Fixtures.delete;
import static io.partdb.storage.Fixtures.keys;
import static io.partdb.storage.Fixtures.put;
import static io.partdb.storage.Fixtures.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreTest {

    @TempDir
    Path directory;

    @Test
    void freshStoreStartsAtRevisionZero() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(0, store.appliedRevision());
            assertEquals(0, store.durableRevision());
            assertNull(value(store, "missing"));
        }
    }

    @Test
    void putsAndDeletesCarryTheirApplyRevision() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(3, List.of(put("a", "one")));
            store.apply(7, List.of(put("a", "two"), put("b", "bee")));
            store.apply(9, List.of(delete("b")));

            try (Snapshot snapshot = store.snapshot()) {
                KeyValue a = snapshot.get(bytes("a")).orElseThrow();
                assertEquals("two", a.value().utf8());
                assertEquals(7, a.revision());
                assertTrue(snapshot.get(bytes("b")).isEmpty());
                assertEquals(9, snapshot.revision());
            }
        }
    }

    @Test
    void applyRejectsRevisionsAtOrBelowTheAppliedRevision() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(5, List.of(put("a", "one")));

            assertThrows(IllegalArgumentException.class, () -> store.apply(5, List.of(put("a", "two"))));
            assertThrows(IllegalArgumentException.class, () -> store.apply(4, List.of()));
            assertEquals("one", value(store, "a"));
            assertEquals(5, store.appliedRevision());
        }
    }

    @Test
    void applyRejectsDuplicateKeysWithoutApplyingAnything() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertThrows(
                IllegalArgumentException.class,
                () -> store.apply(1, List.of(put("a", "one"), put("b", "two"), delete("a")))
            );

            assertEquals(0, store.appliedRevision());
            assertNull(value(store, "a"));
            assertNull(value(store, "b"));
            store.apply(1, List.of(put("b", "two")));
            assertEquals("two", value(store, "b"));
        }
    }

    @Test
    void emptyApplyAdvancesTheRevisionWithoutChangingData() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "one")));
            store.apply(2, List.of());
            store.apply(3, List.of());

            assertEquals(3, store.appliedRevision());
            try (Snapshot snapshot = store.snapshot()) {
                assertEquals(3, snapshot.revision());
                assertEquals(1, snapshot.get(bytes("a")).orElseThrow().revision());
            }
        }
    }

    @Test
    void emptyApplyIsPersistedAsTheDurableRevisionAcrossReopen() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "one")));
            store.apply(2, List.of());
            store.apply(3, List.of());
        }

        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(3, store.appliedRevision());
            assertEquals(3, store.durableRevision());
            assertEquals("one", value(store, "a"));
            store.apply(4, List.of());
        }

        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(4, store.durableRevision());
            assertEquals(4, store.appliedRevision());
        }
    }

    @Test
    void durableRevisionTracksTheRevisionOfFlushedMemtables() {
        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            long revision = Fixtures.fill(store, 0, 400, "value-");
            store.apply(++revision, List.of());
            Fixtures.await("a flush", () -> store.durableRevision() > 0);

            assertTrue(store.durableRevision() <= store.appliedRevision());
            assertTrue(store.stats().tableCount() > 0);
        }
    }

    @Test
    void dataSurvivesReopenAcrossManyTables() {
        long revision;
        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            revision = Fixtures.fill(store, 0, 2_000, "v");
            for (int i = 0; i < 2_000; i += 3) {
                store.apply(++revision, List.of(Mutation.delete(bytes(Fixtures.key(i)))));
            }
        }

        try (Store store = Store.open(directory, Fixtures.SMALL); Snapshot snapshot = store.snapshot()) {
            assertEquals(revision, store.appliedRevision());
            assertEquals(revision, store.durableRevision());
            for (int i = 0; i < 2_000; i++) {
                String expected = i % 3 == 0 ? null : "v" + i;
                assertEquals(expected, value(snapshot, Fixtures.key(i)), Fixtures.key(i));
            }
        }
    }

    @Test
    void writesStallAtTheLevelZeroStopTriggerUntilCompactionDrainsIt() {
        long revision;
        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            revision = Fixtures.fill(store, 0, 400, "v");
        }
        StoreOptions stalling = Fixtures.SMALL.withLevelZeroTriggers(2, 3);

        try (Store store = Store.open(directory, stalling)) {
            store.apply(++revision, List.of(put("after", "stall")));

            assertEquals(1, store.stats().stalledWriteCount());
            assertTrue(store.stats().levelZeroTableCount() < 3);
            assertEquals("stall", value(store, "after"));
            assertEquals("v7", value(store, Fixtures.key(7)));
        }
    }

    @Test
    void scansHonourRangeBoundsAndSkipTombstones() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "1"), put("b", "2"), put("c", "3"), put("d", "4")));
            store.apply(2, List.of(delete("c")));

            try (Snapshot snapshot = store.snapshot()) {
                assertEquals(List.of("a", "b", "d"), keys(snapshot, KeyRange.all()));
                assertEquals(List.of("b", "d"), keys(snapshot, KeyRange.from(bytes("b"))));
                assertEquals(List.of("a", "b"), keys(snapshot, KeyRange.until(bytes("c"))));
                assertEquals(List.of("b"), keys(snapshot, KeyRange.between(bytes("b"), bytes("c"))));
                assertEquals(List.of(), keys(snapshot, KeyRange.between(bytes("b"), bytes("b"))));
            }
        }
    }

    @Test
    void scansMergeMemtablesAndTablesInKeyOrder() {
        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            long revision = Fixtures.fill(store, 0, 500, "old-");
            for (int i = 0; i < 500; i += 2) {
                store.apply(++revision, List.of(put(i, "new-" + i)));
            }

            List<KeyValue> entries = Fixtures.entries(store);
            assertEquals(500, entries.size());
            for (int i = 0; i < 500; i++) {
                assertEquals(Fixtures.key(i), entries.get(i).key().utf8());
                assertEquals((i % 2 == 0 ? "new-" : "old-") + i, entries.get(i).value().utf8());
            }
        }
    }

    @Test
    void snapshotsKeepTheirRevisionWhileWritesContinue() {
        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            store.apply(1, List.of(put("a", "one"), put("b", "bee")));
            try (Snapshot snapshot = store.snapshot()) {
                store.apply(2, List.of(put("a", "two"), delete("b"), put("c", "sea")));
                Fixtures.fill(store, 2, 1_000, "filler-");

                assertEquals(1, snapshot.revision());
                assertEquals("one", value(snapshot, "a"));
                assertEquals("bee", value(snapshot, "b"));
                assertNull(value(snapshot, "c"));
                assertEquals(List.of("a", "b"), keys(snapshot, KeyRange.until(bytes("key"))));
            }
            assertEquals("two", value(store, "a"));
            assertNull(value(store, "b"));
        }
    }

    @Test
    void scanRemainsUsableAfterItsSnapshotCloses() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "1"), put("b", "2")));
            Stream<KeyValue> scan;
            try (Snapshot snapshot = store.snapshot()) {
                scan = snapshot.scan(KeyRange.all());
            }
            try (scan) {
                assertEquals(List.of("a", "b"), scan.map(entry -> entry.key().utf8()).toList());
            }
            assertThrows(IllegalStateException.class, scan::toList);
        }
    }

    @Test
    void closedSnapshotRejectsReads() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            Snapshot snapshot = store.snapshot();
            snapshot.close();

            assertThrows(IllegalStateException.class, () -> snapshot.get(bytes("a")));
            assertThrows(IllegalStateException.class, () -> snapshot.scan(KeyRange.all()));
            snapshot.close();
        }
    }

    @Test
    void closedStoreRejectsOperations() {
        Store store = Store.open(directory, StoreOptions.defaults());
        store.close();

        assertThrows(StoreException.Closed.class, () -> store.apply(1, List.of()));
        assertThrows(StoreException.Closed.class, store::snapshot);
        store.close();
    }
}
