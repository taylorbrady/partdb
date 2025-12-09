package io.partdb.common;

import java.util.Arrays;
import java.util.Objects;

public record Entry(
    byte[] key,
    byte[] value,
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
        key = key.clone();
        value = value != null ? value.clone() : null;
    }

    public static Entry put(byte[] key, byte[] value, long version) {
        return new Entry(key, value, version, false, 0);
    }

    public static Entry putWithLease(byte[] key, byte[] value, long version, long leaseId) {
        return new Entry(key, value, version, false, leaseId);
    }

    public static Entry delete(byte[] key, long version) {
        return new Entry(key, null, version, true, 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Entry e)) return false;
        return version == e.version
            && tombstone == e.tombstone
            && leaseId == e.leaseId
            && Arrays.equals(key, e.key)
            && Arrays.equals(value, e.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Long.hashCode(version);
        result = 31 * result + Boolean.hashCode(tombstone);
        result = 31 * result + Long.hashCode(leaseId);
        return result;
    }
}
