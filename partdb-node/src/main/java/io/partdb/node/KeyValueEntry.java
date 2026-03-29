package io.partdb.node;

import java.util.Objects;

public record KeyValueEntry(
    byte[] key,
    byte[] value,
    long version,
    long leaseId
) {
    public KeyValueEntry {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
    }
}
