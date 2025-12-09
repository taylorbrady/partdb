package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;

public record VersionedKey(ByteArray key, Timestamp timestamp) implements Comparable<VersionedKey> {

    @Override
    public int compareTo(VersionedKey other) {
        int cmp = key.compareTo(other.key);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compareUnsigned(other.timestamp.value(), this.timestamp.value());
    }
}
