package io.partdb.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class EngineEntryTest {

    @Test
    void recordAccessors() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");
        long revision = 1000L;

        EngineEntry entry = new EngineEntry(key, value, revision);

        assertEquals(key, entry.key());
        assertEquals(value, entry.value());
        assertEquals(revision, entry.revision());
    }

    @Test
    void equalsAndHashCode() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");

        EngineEntry entry1 = new EngineEntry(key, value, 1000L);
        EngineEntry entry2 = new EngineEntry(key, value, 1000L);

        assertEquals(entry1, entry2);
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void notEqualWhenRevisionDiffers() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");

        EngineEntry entry1 = new EngineEntry(key, value, 1000L);
        EngineEntry entry2 = new EngineEntry(key, value, 2000L);

        assertNotEquals(entry1, entry2);
    }

    @Test
    void notEqualWhenKeyDiffers() {
        Slice value = Slice.of("value");

        EngineEntry entry1 = new EngineEntry(Slice.of("key1"), value, 1000L);
        EngineEntry entry2 = new EngineEntry(Slice.of("key2"), value, 1000L);

        assertNotEquals(entry1, entry2);
    }

    @Test
    void notEqualWhenValueDiffers() {
        Slice key = Slice.of("key");

        EngineEntry entry1 = new EngineEntry(key, Slice.of("value1"), 1000L);
        EngineEntry entry2 = new EngineEntry(key, Slice.of("value2"), 1000L);

        assertNotEquals(entry1, entry2);
    }
}
