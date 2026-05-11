package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record VersionedValue(Bytes value, long modRevision) {
    public VersionedValue {
        value = Objects.requireNonNull(value, "value must not be null");
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
