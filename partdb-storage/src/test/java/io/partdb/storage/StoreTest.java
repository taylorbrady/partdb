package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
    void freshStoreStartsAtIndexZero() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(0, store.appliedIndex());
            assertEquals(0, store.durableIndex());
            assertTrue(store.get(bytes("missing")).isEmpty());
        }
    }

    @Test
    void putsAndDeletesCarryTheirApplyIndexAsRevision() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(3, List.of(put("a", "one")));
            store.apply(7, List.of(put("a", "two"), put("b", "bee")));
            store.apply(9, List.of(delete("b")));

            KeyValue a = store.get(bytes("a")).orElseThrow();
            assertEquals("two", a.value().utf8());
            assertEquals(7, a.revision());
            assertTrue(store.get(bytes("b")).isEmpty());
            assertEquals(9, store.appliedIndex());
        }
    }

    @Test
    void applyRejectsIndexesAtOrBelowTheAppliedIndex() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(5, List.of(put("a", "one")));

            assertThrows(IllegalArgumentException.class, () -> store.apply(5, List.of(put("a", "two"))));
            assertThrows(IllegalArgumentException.class, () -> store.apply(4, List.of()));
            assertEquals("one", value(store, "a"));
            assertEquals(5, store.appliedIndex());
        }
    }

    @Test
    void applyRejectsDuplicateKeysWithoutApplyingAnything() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertThrows(
                IllegalArgumentException.class,
                () -> store.apply(1, List.of(put("a", "one"), put("b", "two"), delete("a")))
            );

            assertEquals(0, store.appliedIndex());
            assertNull(value(store, "a"));
            assertNull(value(store, "b"));
            store.apply(1, List.of(put("b", "two")));
            assertEquals("two", value(store, "b"));
        }
    }

    @Test
    void emptyApplyAdvancesAppliedIndexWithoutChangingData() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "one")));
            store.apply(2, List.of());
            store.apply(3, List.of());

            assertEquals(3, store.appliedIndex());
            assertEquals(1, store.get(bytes("a")).orElseThrow().revision());
            try (Snapshot snapshot = store.snapshot()) {
                assertEquals(3, snapshot.index());
            }
        }
    }

    @Test
    void emptyApplyIsPersistedAsTheDurableIndexAcrossReopen() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "one")));
            store.apply(2, List.of());
            store.apply(3, List.of());
        }

        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(3, store.appliedIndex());
            assertEquals(3, store.durableIndex());
            assertEquals("one", value(store, "a"));
            store.apply(4, List.of());
        }

        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            assertEquals(4, store.durableIndex());
            assertEquals(4, store.appliedIndex());
        }
    }

    @Test
    void durableIndexTracksTheAppliedIndexOfFlushedMemtables() {
        try (Store store = Store.open(directory, Fixtures.NO_COMPACTION)) {
            long index = Fixtures.fill(store, 0, 400, "value-");
            store.apply(++index, List.of());
            Fixtures.await("a flush", () -> store.durableIndex() > 0);

            assertTrue(store.durableIndex() <= store.appliedIndex());
            assertTrue(store.stats().tableCount() > 0);
        }
    }

    @Test
    void dataSurvivesReopenAcrossManyTables() {
        long index;
        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            index = Fixtures.fill(store, 0, 2_000, "v");
            for (int i = 0; i < 2_000; i += 3) {
                store.apply(++index, List.of(Mutation.delete(bytes(Fixtures.key(i)))));
            }
        }

        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            assertEquals(index, store.appliedIndex());
            assertEquals(index, store.durableIndex());
            for (int i = 0; i < 2_000; i++) {
                String expected = i % 3 == 0 ? null : "v" + i;
                assertEquals(expected, value(store, Fixtures.key(i)), Fixtures.key(i));
            }
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
            long index = Fixtures.fill(store, 0, 500, "old-");
            for (int i = 0; i < 500; i += 2) {
                store.apply(++index, List.of(put(i, "new-" + i)));
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
    void snapshotsKeepTheirReadPointWhileWritesContinue() {
        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            store.apply(1, List.of(put("a", "one"), put("b", "bee")));
            try (Snapshot snapshot = store.snapshot()) {
                store.apply(2, List.of(put("a", "two"), delete("b"), put("c", "sea")));
                Fixtures.fill(store, 2, 1_000, "filler-");

                assertEquals(1, snapshot.index());
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
    void cursorRemainsUsableAfterItsSnapshotCloses() {
        try (Store store = Store.open(directory, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "1"), put("b", "2")));
            Cursor cursor;
            try (Snapshot snapshot = store.snapshot()) {
                cursor = snapshot.scan(KeyRange.all());
            }
            List<String> seen = new ArrayList<>();
            try (cursor) {
                cursor.forEachRemaining(entry -> seen.add(entry.key().utf8()));
            }
            assertEquals(List.of("a", "b"), seen);
            assertThrows(IllegalStateException.class, cursor::hasNext);
        }
    }

    @Test
    void closedStoreRejectsOperations() {
        Store store = Store.open(directory, StoreOptions.defaults());
        store.close();

        assertThrows(StoreException.Closed.class, () -> store.apply(1, List.of()));
        assertThrows(StoreException.Closed.class, () -> store.get(bytes("a")));
        assertThrows(StoreException.Closed.class, store::snapshot);
        store.close();
    }
}
