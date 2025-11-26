package io.partdb.common;

import java.util.Objects;

public record Entry(
    ByteArray key,
    ByteArray value,
    long version,
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

    public static Entry put(ByteArray key, ByteArray value, long version) {
        return new Entry(key, value, version, false, 0);
    }

    public static Entry putWithLease(ByteArray key, ByteArray value, long version, long leaseId) {
        return new Entry(key, value, version, false, leaseId);
    }

    public static Entry delete(ByteArray key, long version) {
        return new Entry(key, null, version, true, 0);
    }

    public KeyValue toKeyValue() {
        if (tombstone) {
            throw new IllegalStateException("Cannot convert tombstone to KeyValue");
        }
        return new KeyValue(key, value);
    }
}
