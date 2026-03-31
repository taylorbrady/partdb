package io.partdb.client;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record KeyValue(Bytes key, Bytes value, long revision) {
    public KeyValue {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
    }
}
