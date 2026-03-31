package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record VersionedValue(Bytes value, long revision) {

    public VersionedValue {
        value = Objects.requireNonNull(value, "value must not be null");
    }
}
