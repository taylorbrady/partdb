package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IteratorTest {

    @Test
    void mergingIteratorOrdersByKeyThenNewestRevision() {
        var merged = new MergingIterator(List.of(
            List.of(value("a", 5), value("c", 1)).iterator(),
            List.of(value("a", 7), tombstone("b", 3)).iterator(),
            List.<InternalEntry>of().iterator()
        ));

        assertEquals(List.of(value("a", 7), value("a", 5), tombstone("b", 3), value("c", 1)), drain(merged));
    }

    @Test
    void latestIteratorPicksNewestVisibleVersionPerKey() {
        List<InternalEntry> history = List.of(
            value("a", 9), value("a", 4), tombstone("b", 6), value("b", 2), value("c", 8), tombstone("d", 3)
        );

        assertEquals(List.of(value("a", 4), value("b", 2), tombstone("d", 3)), drain(new LatestIterator(history.iterator(), 5, false)));
        assertEquals(List.of(value("a", 4), value("b", 2)), drain(new LatestIterator(history.iterator(), 5, true)));
        assertEquals(List.of(value("a", 9), value("c", 8)), drain(new LatestIterator(history.iterator(), Long.MAX_VALUE, true)));
    }

    @Test
    void memtableReturnsNewestEntryAtOrBelowTheReadIndex() {
        Memtable memtable = new Memtable(0);
        memtable.put(value("a", 2));
        memtable.put(tombstone("a", 4));
        memtable.put(value("b", 3));
        memtable.advanceTo(4);

        assertEquals(value("a", 2), memtable.get(Slice.utf8("a"), 3).orElseThrow());
        assertEquals(tombstone("a", 4), memtable.get(Slice.utf8("a"), 4).orElseThrow());
        assertEquals(List.of(), memtable.get(Slice.utf8("a"), 1).stream().toList());
        assertEquals(List.of(value("b", 3)), drain(memtable.scan(new Bounds(Slice.utf8("b"), null))));
        assertEquals(List.of(tombstone("a", 4), value("a", 2)), drain(memtable.scan(new Bounds(null, Slice.utf8("b")))));
        assertEquals(4, memtable.appliedIndex());
    }

    private static InternalEntry value(String key, long revision) {
        return new InternalEntry.Value(new InternalKey(Slice.utf8(key), revision), Slice.utf8(key + revision));
    }

    private static InternalEntry tombstone(String key, long revision) {
        return new InternalEntry.Tombstone(new InternalKey(Slice.utf8(key), revision));
    }

    private static List<InternalEntry> drain(Iterator<InternalEntry> iterator) {
        List<InternalEntry> entries = new ArrayList<>();
        iterator.forEachRemaining(entries::add);
        return entries;
    }
}
