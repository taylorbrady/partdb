package io.partdb.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class StoredEntryValueTest {

    @Test
    void recordAccessors() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");
        long revision = 1000L;

        StoredEntry.Value entry = new StoredEntry.Value(key, value, revision);

        assertEquals(key, entry.key());
        assertEquals(value, entry.value());
        assertEquals(revision, entry.revision());
    }

    @Test
    void equalsAndHashCode() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");

        StoredEntry.Value entry1 = new StoredEntry.Value(key, value, 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(key, value, 1000L);

        assertEquals(entry1, entry2);
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void notEqualWhenRevisionDiffers() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");

        StoredEntry.Value entry1 = new StoredEntry.Value(key, value, 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(key, value, 2000L);

        assertNotEquals(entry1, entry2);
    }

    @Test
    void notEqualWhenKeyDiffers() {
        Slice value = Slice.of("value");

        StoredEntry.Value entry1 = new StoredEntry.Value(Slice.of("key1"), value, 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(Slice.of("key2"), value, 1000L);

        assertNotEquals(entry1, entry2);
    }

    @Test
    void notEqualWhenValueDiffers() {
        Slice key = Slice.of("key");

        StoredEntry.Value entry1 = new StoredEntry.Value(key, Slice.of("value1"), 1000L);
        StoredEntry.Value entry2 = new StoredEntry.Value(key, Slice.of("value2"), 1000L);

        assertNotEquals(entry1, entry2);
    }
}
