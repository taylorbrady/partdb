package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InternalEntryMergingIteratorTest {

    @Test
    void mergesInternalEntriesInInternalKeyOrder() {
        InternalEntryMergingIterator iterator = new InternalEntryMergingIterator(List.of(
            entries(
                value("a", "v3", 3),
                value("c", "v1", 1)
            ).iterator(),
            entries(
                value("a", "v2", 2),
                tombstone("b", 4)
            ).iterator(),
            entries(
                value("a", "v1", 1),
                value("d", "v1", 1)
            ).iterator()
        ));

        List<String> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalEntry entry = iterator.next();
            entries.add("%s@%d".formatted(utf8(entry.userKey()), entry.revision()));
        }

        assertEquals(List.of("a@3", "a@2", "a@1", "b@4", "c@1", "d@1"), entries);
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
