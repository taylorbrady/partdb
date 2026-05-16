package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class StoredEntryValueTest {

    @Test
    void recordAccessors() {
        Slice key = Slice.utf8("key");
        Slice value = Slice.utf8("value");
        long revision = 1000L;

        StoredEntry.Value entry = new StoredEntry.Value(key, value, revision);

        assertEquals(key, entry.key());
        assertEquals(value, entry.value());
        assertEquals(revision, entry.revision());
    }

    @Test
    void equalsAndHashCode() {
        Slice key = Slice.utf8("key");
        Slice value = Slice.utf8("value");

        StoredEntry.Value entry1 = new StoredEntry.Value(key, value, 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(key, value, 1000L);

        assertEquals(entry1, entry2);
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void notEqualWhenRevisionDiffers() {
        Slice key = Slice.utf8("key");
        Slice value = Slice.utf8("value");

        StoredEntry.Value entry1 = new StoredEntry.Value(key, value, 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(key, value, 2000L);

        assertNotEquals(entry1, entry2);
    }

    @Test
    void notEqualWhenKeyDiffers() {
        Slice value = Slice.utf8("value");

        StoredEntry.Value entry1 = new StoredEntry.Value(Slice.utf8("key1"), value, 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(Slice.utf8("key2"), value, 1000L);

        assertNotEquals(entry1, entry2);
    }

    @Test
    void notEqualWhenValueDiffers() {
        Slice key = Slice.utf8("key");

        StoredEntry.Value entry1 = new StoredEntry.Value(key, Slice.utf8("value1"), 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(key, Slice.utf8("value2"), 1000L);

        assertNotEquals(entry1, entry2);
    }
}
