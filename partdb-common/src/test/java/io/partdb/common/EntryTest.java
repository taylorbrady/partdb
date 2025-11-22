package io.partdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class EntryTest {

    @Test
    void putFactoryCreatesNonTombstoneEntry() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        long timestamp = 1000L;

        Entry entry = Entry.put(key, value, timestamp);

        assertThat(entry.key()).isEqualTo(key);
        assertThat(entry.value()).isEqualTo(value);
        assertThat(entry.timestamp()).isEqualTo(timestamp);
        assertThat(entry.tombstone()).isFalse();
        assertThat(entry.expiresAtMillis()).isZero();
    }

    @Test
    void putWithTTLSetsExpirationTime() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        long timestamp = 1000L;
        long ttl = 5000L;

        Entry entry = Entry.putWithTTL(key, value, timestamp, ttl);

        assertThat(entry.expiresAtMillis()).isEqualTo(1000L + 5000L);
        assertThat(entry.tombstone()).isFalse();
    }

    @Test
    void deleteFactoryCreatesTombstoneEntry() {
        ByteArray key = ByteArray.of((byte) 1);
        long timestamp = 2000L;

        Entry entry = Entry.delete(key, timestamp);

        assertThat(entry.key()).isEqualTo(key);
        assertThat(entry.value()).isNull();
        assertThat(entry.timestamp()).isEqualTo(timestamp);
        assertThat(entry.tombstone()).isTrue();
        assertThat(entry.expiresAtMillis()).isZero();
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
    void isExpiredReturnsFalseWhenNoTTL() {
        Entry entry = Entry.put(
            ByteArray.of((byte) 1),
            ByteArray.of((byte) 2),
            1000L
        );

        assertThat(entry.isExpired(10000L)).isFalse();
    }

    @Test
    void isExpiredReturnsFalseBeforeExpiration() {
        Entry entry = Entry.putWithTTL(
            ByteArray.of((byte) 1),
            ByteArray.of((byte) 2),
            1000L,
            5000L
        );

        assertThat(entry.isExpired(5999L)).isFalse();
    }

    @Test
    void isExpiredReturnsTrueAtExactExpirationTime() {
        Entry entry = Entry.putWithTTL(
            ByteArray.of((byte) 1),
            ByteArray.of((byte) 2),
            1000L,
            5000L
        );

        assertThat(entry.isExpired(6000L)).isTrue();
    }

    @Test
    void isExpiredReturnsTrueAfterExpiration() {
        Entry entry = Entry.putWithTTL(
            ByteArray.of((byte) 1),
            ByteArray.of((byte) 2),
            1000L,
            5000L
        );

        assertThat(entry.isExpired(10000L)).isTrue();
    }

    @Test
    void toKVPairConvertsSuccessfully() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        Entry entry = Entry.put(key, value, 1000L);

        KVPair pair = entry.toKVPair();

        assertThat(pair.key()).isEqualTo(key);
        assertThat(pair.value()).isEqualTo(value);
    }

    @Test
    void toKVPairThrowsForTombstone() {
        Entry entry = Entry.delete(ByteArray.of((byte) 1), 1000L);

        assertThatThrownBy(entry::toKVPair)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Cannot convert tombstone to KVPair");
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
    void notEqualWhenExpirationDiffers() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        Entry entry1 = Entry.put(key, value, 1000L);
        Entry entry2 = Entry.putWithTTL(key, value, 1000L, 5000L);

        assertThat(entry1).isNotEqualTo(entry2);
    }
}
