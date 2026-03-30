package io.partdb.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class StorageEntryTest {

    @Test
    void recordAccessors() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");
        long revision = 1000L;

        StorageEntry entry = new StorageEntry(key, value, revision);

        assertThat(entry.key()).isEqualTo(key);
        assertThat(entry.value()).isEqualTo(value);
        assertThat(entry.revision()).isEqualTo(revision);
    }

    @Test
    void equalsAndHashCode() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");

        StorageEntry entry1 = new StorageEntry(key, value, 1000L);
        StorageEntry entry2 = new StorageEntry(key, value, 1000L);

        assertThat(entry1).isEqualTo(entry2);
        assertThat(entry1.hashCode()).isEqualTo(entry2.hashCode());
    }

    @Test
    void notEqualWhenRevisionDiffers() {
        Slice key = Slice.of("key");
        Slice value = Slice.of("value");

        StorageEntry entry1 = new StorageEntry(key, value, 1000L);
        StorageEntry entry2 = new StorageEntry(key, value, 2000L);

        assertThat(entry1).isNotEqualTo(entry2);
    }

    @Test
    void notEqualWhenKeyDiffers() {
        Slice value = Slice.of("value");

        StorageEntry entry1 = new StorageEntry(Slice.of("key1"), value, 1000L);
        StorageEntry entry2 = new StorageEntry(Slice.of("key2"), value, 1000L);

        assertThat(entry1).isNotEqualTo(entry2);
    }

    @Test
    void notEqualWhenValueDiffers() {
        Slice key = Slice.of("key");

        StorageEntry entry1 = new StorageEntry(key, Slice.of("value1"), 1000L);
        StorageEntry entry2 = new StorageEntry(key, Slice.of("value2"), 1000L);

        assertThat(entry1).isNotEqualTo(entry2);
    }
}
