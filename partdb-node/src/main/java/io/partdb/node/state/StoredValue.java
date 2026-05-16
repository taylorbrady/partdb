package io.partdb.node.state;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record StoredValue(Bytes value, long revision) {
    public StoredValue {
        value = Objects.requireNonNull(value, "value must not be null");
        if (revision <= 0) {
            throw new IllegalArgumentException("revision must be positive");
        }
    }
}
