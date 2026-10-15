package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record KeyValue(Bytes key, Bytes value, long revision) {
    public KeyValue {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
    }
}
