package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VisibleEntryIteratorTest {

    @Test
    void emitsLatestVisibleValuePerKeyAtSnapshot() {
        VisibleEntryIterator iterator = new VisibleEntryIterator(List.of(
            entries(
                value("a", "newest", 5),
                value("b", "visible", 4),
                value("c", "masked", 3)
            ).iterator(),
            entries(
                value("a", "visible", 3),
                tombstone("c", 2)
            ).iterator(),
            entries(
                value("a", "old", 1),
                value("d", "visible", 1)
            ).iterator()
        ), 4);

        List<String> visible = new ArrayList<>();
        while (iterator.hasNext()) {
            StoredEntry.Value entry = iterator.next();
            visible.add("%s=%s@%d".formatted(utf8(entry.key()), utf8(entry.value()), entry.revision()));
        }

        assertEquals(List.of("a=visible@3", "b=visible@4", "c=masked@3", "d=visible@1"), visible);
    }

    @Test
    void skipsEntriesNewerThanSnapshotButStillEmitsOlderVisibleVersion() {
        VisibleEntryIterator iterator = new VisibleEntryIterator(List.of(
            entries(
                value("a", "too-new", 5),
                value("a", "visible", 3),
                value("b", "visible", 4)
            ).iterator(),
            entries(
                value("c", "visible", 2)
            ).iterator()
        ), 4);

        List<String> visible = new ArrayList<>();
        while (iterator.hasNext()) {
            StoredEntry.Value entry = iterator.next();
            visible.add("%s=%s@%d".formatted(utf8(entry.key()), utf8(entry.value()), entry.revision()));
        }

        assertEquals(List.of("a=visible@3", "b=visible@4", "c=visible@2"), visible);
    }

    @Test
    void tombstoneHidesOlderValuesForSameKey() {
        VisibleEntryIterator iterator = new VisibleEntryIterator(List.of(
            entries(
                tombstone("a", 4),
                value("b", "value", 3)
            ).iterator(),
            entries(
                value("a", "older", 2),
                value("c", "value", 1)
            ).iterator()
        ), Long.MAX_VALUE);

        List<String> visible = new ArrayList<>();
        while (iterator.hasNext()) {
            StoredEntry.Value entry = iterator.next();
            visible.add(utf8(entry.key()));
        }

        assertEquals(List.of("b", "c"), visible);
    }

    private static InternalEntry.Value value(String key, String value, long revision) {
        return new InternalEntry.Value(new InternalKey(Slice.utf8(key), revision), Slice.utf8(value));
    }

    private static InternalEntry.Tombstone tombstone(String key, long revision) {
        return new InternalEntry.Tombstone(new InternalKey(Slice.utf8(key), revision));
    }

    private static List<InternalEntry> entries(InternalEntry... entries) {
        return List.of(entries);
    }

    private static String utf8(Slice value) {
        return new String(value.toByteArray());
    }
}
