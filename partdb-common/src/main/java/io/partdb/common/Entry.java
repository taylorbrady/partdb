package io.partdb.common;

import java.util.Objects;

public record Entry(
    ByteArray key,
    ByteArray value,
    long timestamp,
    boolean tombstone,
    long leaseId
) {

    public Entry {
        Objects.requireNonNull(key, "key cannot be null");
        if (!tombstone && value == null) {
            throw new IllegalArgumentException("value cannot be null for non-tombstone");
        }
        if (leaseId < 0) {
            throw new IllegalArgumentException("leaseId must be non-negative");
        }
    }

    public static Entry put(ByteArray key, ByteArray value, long timestamp) {
        return new Entry(key, value, timestamp, false, 0);
    }

    public static Entry putWithLease(ByteArray key, ByteArray value, long timestamp, long leaseId) {
        return new Entry(key, value, timestamp, false, leaseId);
    }

    public static Entry delete(ByteArray key, long timestamp) {
        return new Entry(key, null, timestamp, true, 0);
    }

    public KVPair toKVPair() {
        if (tombstone) {
            throw new IllegalStateException("Cannot convert tombstone to KVPair");
        }
        return new KVPair(key, value);
    }
}
