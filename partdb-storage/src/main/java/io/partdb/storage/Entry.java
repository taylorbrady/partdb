package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record Entry(Bytes key, Bytes value, long revision) {

    public Entry {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
    }
}
