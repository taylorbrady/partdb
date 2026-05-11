package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record KeyValueEntry(
    Bytes key,
    Bytes value,
    long modRevision
) {
    public KeyValueEntry {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
