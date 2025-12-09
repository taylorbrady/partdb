package io.partdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class EntryTest {

    @Test
    void putFactoryCreatesNonTombstoneEntry() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        long version = 1000L;

        Entry entry = Entry.put(key, value, version);

        assertThat(entry.key()).isEqualTo(key);
        assertThat(entry.value()).isEqualTo(value);
        assertThat(entry.version()).isEqualTo(version);
        assertThat(entry.tombstone()).isFalse();
        assertThat(entry.leaseId()).isZero();
    }

    @Test
    void putWithLeaseAttachesLease() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        long version = 1000L;
        long leaseId = 42L;

        Entry entry = Entry.putWithLease(key, value, version, leaseId);

        assertThat(entry.leaseId()).isEqualTo(leaseId);
        assertThat(entry.tombstone()).isFalse();
    }

    @Test
    void deleteFactoryCreatesTombstoneEntry() {
        ByteArray key = ByteArray.of((byte) 1);
        long version = 2000L;

        Entry entry = Entry.delete(key, version);

        assertThat(entry.key()).isEqualTo(key);
        assertThat(entry.value()).isNull();
        assertThat(entry.version()).isEqualTo(version);
        assertThat(entry.tombstone()).isTrue();
        assertThat(entry.leaseId()).isZero();
    }

    @Test
    void constructorThrowsWhenKeyIsNull() {
        ByteArray value = ByteArray.of((byte) 1);

        assertThatThrownBy(() -> new Entry(null, value, 1000L, false, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("key cannot be null");
    }

    @Test
    void constructorThrowsWhenNonTombstoneHasNullValue() {
        ByteArray key = ByteArray.of((byte) 1);

        assertThatThrownBy(() -> new Entry(key, null, 1000L, false, 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("value cannot be null for non-tombstone");
    }

    @Test
    void constructorAllowsNullValueForTombstone() {
        ByteArray key = ByteArray.of((byte) 1);

        Entry entry = new Entry(key, null, 1000L, true, 0);

        assertThat(entry.tombstone()).isTrue();
        assertThat(entry.value()).isNull();
    }

    @Test
    void equalsWorksForSameContent() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        Entry entry1 = Entry.put(key, value, 1000L);
        Entry entry2 = Entry.put(key, value, 1000L);

        assertThat(entry1).isEqualTo(entry2);
        assertThat(entry1.hashCode()).isEqualTo(entry2.hashCode());
    }

    @Test
    void notEqualWhenTimestampDiffers() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        Entry entry1 = Entry.put(key, value, 1000L);
        Entry entry2 = Entry.put(key, value, 2000L);

        assertThat(entry1).isNotEqualTo(entry2);
    }

    @Test
    void notEqualWhenTombstoneFlagDiffers() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        Entry entry1 = Entry.put(key, value, 1000L);
        Entry entry2 = Entry.delete(key, 1000L);

        assertThat(entry1).isNotEqualTo(entry2);
    }

    @Test
    void notEqualWhenLeaseIdDiffers() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        Entry entry1 = Entry.put(key, value, 1000L);
        Entry entry2 = Entry.putWithLease(key, value, 1000L, 42L);

        assertThat(entry1).isNotEqualTo(entry2);
    }
}
