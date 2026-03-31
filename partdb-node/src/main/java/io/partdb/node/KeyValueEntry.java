package io.partdb.node;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record KeyValueEntry(
    Bytes key,
    Bytes value,
    long version,
    long leaseId
) {
    public KeyValueEntry {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
    }
}
