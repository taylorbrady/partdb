package io.partdb.common;

import java.util.Objects;

public record Entry(
    ByteArray key,
    ByteArray value,
    long timestamp,
    boolean tombstone,
    long expiresAtMillis
) {

    public Entry {
        Objects.requireNonNull(key, "key cannot be null");
        if (!tombstone && value == null) {
            throw new IllegalArgumentException("value cannot be null for non-tombstone");
        }
    }

    public static Entry put(ByteArray key, ByteArray value, long timestamp) {
        return new Entry(key, value, timestamp, false, 0);
    }

    public static Entry putWithTTL(ByteArray key, ByteArray value, long timestamp, long ttlMillis) {
        return new Entry(key, value, timestamp, false, timestamp + ttlMillis);
    }

    public static Entry putWithExpiry(ByteArray key, ByteArray value, long timestamp, long expiresAtMillis) {
        return new Entry(key, value, timestamp, false, expiresAtMillis);
    }

    public static Entry delete(ByteArray key, long timestamp) {
        return new Entry(key, null, timestamp, true, 0);
    }

    public boolean isExpired(long currentTimeMillis) {
        return expiresAtMillis > 0 && currentTimeMillis >= expiresAtMillis;
    }

    public KVPair toKVPair() {
        if (tombstone) {
            throw new IllegalStateException("Cannot convert tombstone to KVPair");
        }
        return new KVPair(key, value);
    }
}
