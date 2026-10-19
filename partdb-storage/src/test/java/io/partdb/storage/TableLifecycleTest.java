package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.partdb.storage.Fixtures.tableFiles;
import static io.partdb.storage.Fixtures.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableLifecycleTest {

    @TempDir
    Path directory;

    @Test
    void releasingTheLastVersionUnmapsAndDeletesAnObsoleteTable() {
        TableReader table = write(directory.resolve("000001.sst"), 1, "a");
        Version first = Version.empty().withFlushed(table);
        table.release();
        Version second = first.retain();

        TableReader replacement = write(directory.resolve("000002.sst"), 2, "b");
        Version compacted = first.withCompaction(List.of(table), List.of(replacement), 1);
        replacement.release();
        table.markObsolete();

        first.release();
        assertTrue(Files.exists(table.path()));
        assertEquals("a", table.get(Slice.utf8("a")).map(Fixtures::valueOf).orElseThrow());

        second.release();
        assertFalse(Files.exists(table.path()));
        assertThrows(IllegalStateException.class, () -> table.contents().get(ValueLayout.JAVA_BYTE, 0));

        compacted.release();
        assertTrue(Files.exists(replacement.path()), "tables that are not obsolete keep their file");
        assertThrows(IllegalStateException.class, () -> replacement.contents().get(ValueLayout.JAVA_BYTE, 0));
    }

    @Test
    void carriedOverTablesKeepExactlyOneReferencePerVersion() {
        TableReader kept = write(directory.resolve("000001.sst"), 1, "a");
        Version first = Version.empty().withFlushed(kept);
        kept.release();
        TableReader flushed = write(directory.resolve("000002.sst"), 2, "b");
        Version second = first.withFlushed(flushed);
        flushed.release();

        first.release();
        assertEquals("a", kept.get(Slice.utf8("a")).map(Fixtures::valueOf).orElseThrow());
        second.release();
        assertThrows(IllegalStateException.class, () -> kept.contents().get(ValueLayout.JAVA_BYTE, 0));
        assertThrows(IllegalStateException.class, kept::release);
    }

    @Test
    void movedTablesKeepTheirFile() {
        TableReader table = write(directory.resolve("000001.sst"), 1, "a");
        Version first = Version.empty().withFlushed(table);
        table.release();

        Version moved = first.withCompaction(List.of(table), List.of(table), 3);
        first.release();

        assertEquals(List.of(), moved.level(0));
        assertEquals(List.of(table), moved.level(3));
        assertEquals("a", table.get(Slice.utf8("a")).map(Fixtures::valueOf).orElseThrow());
        moved.release();
        assertTrue(Files.exists(table.path()));
    }

    @Test
    void versionsRejectOverlappingTablesBelowLevelZero() {
        TableReader left = write(directory.resolve("000001.sst"), 1, "a");
        TableReader right = write(directory.resolve("000002.sst"), 2, "a");
        Version base = Version.empty().withCompaction(List.of(), List.of(left), 2);

        assertThrows(IllegalStateException.class, () -> base.withCompaction(List.of(), List.of(right), 2));

        base.release();
        left.release();
        right.release();
    }

    @Test
    void compactedTablesAreDeletedOnceNoSnapshotPinsThem() {
        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            long revision = Fixtures.fill(store, 0, 200, "first-");
            awaitQuiet(store);

            try (Snapshot pinned = store.snapshot()) {
                List<String> before = live(store);
                long completed = store.stats().completedCompactions();
                Fixtures.fill(store, revision, 400, "second-");
                Fixtures.await("a compaction", () -> store.stats().completedCompactions() > completed);
                Fixtures.await("compactions to settle", () -> store.stats().activeCompactions() == 0);

                assertTrue(before.stream().anyMatch(name -> !live(store).contains(name)), "compaction must retire a pinned table");
                assertTrue(tableFiles(directory).containsAll(before), "pinned tables must survive compaction");
                assertEquals("first-5", value(pinned, Fixtures.key(5)));
                assertEquals(200, Fixtures.keys(pinned, KeyRange.all()).size());

                pinned.close();
                Fixtures.await("retired tables to be deleted", () -> settled(store));
            }
            assertEquals("second-5", value(store, Fixtures.key(5)));
        }
    }

    @Test
    void openScanPinsTablesLikeASnapshot() {
        try (Store store = Store.open(directory, Fixtures.SMALL)) {
            long revision = Fixtures.fill(store, 0, 200, "first-");
            awaitQuiet(store);

            Stream<KeyValue> scan;
            List<String> before;
            try (Snapshot snapshot = store.snapshot()) {
                scan = snapshot.scan(KeyRange.all());
                before = live(store);
            }
            long completed = store.stats().completedCompactions();
            Fixtures.fill(store, revision, 400, "second-");
            Fixtures.await("a compaction", () -> store.stats().completedCompactions() > completed);

            assertTrue(before.stream().anyMatch(name -> !live(store).contains(name)), "compaction must retire a pinned table");
            try (scan) {
                assertTrue(tableFiles(directory).containsAll(before));
                assertTrue(scan.allMatch(entry -> entry.value().utf8().startsWith("first-")));
            }
            Fixtures.await("retired tables to be deleted", () -> settled(store));
        }
    }

    private static void awaitQuiet(Store store) {
        Fixtures.await("tables", () -> store.stats().tableCount() >= 1);
        Fixtures.await("quiet store", () -> {
            StoreStats stats = store.stats();
            return stats.activeCompactions() == 0 && stats.immutableMemtableCount() == 0;
        });
    }

    private static List<String> live(Store store) {
        ReadState state = store.acquire();
        try {
            return state.version().tables().stream().map(table -> table.path().getFileName().toString()).sorted().toList();
        } finally {
            state.release();
        }
    }

    private boolean settled(Store store) {
        return store.stats().activeCompactions() == 0 && tableFiles(directory).equals(live(store));
    }

    private static TableReader write(Path path, long id, String key) {
        try (TableWriter writer = TableWriter.create(path, id, StoreOptions.defaults())) {
            writer.add(new InternalEntry.Value(new InternalKey(Slice.utf8(key), 1), Slice.utf8(key)));
            writer.finish();
        }
        return TableReader.open(path, id, new BlockCache(1024 * 1024));
    }
}
