package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record PutRequest(Bytes key, Bytes value) {
    public PutRequest {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
    }

    public static PutRequest of(Bytes key, Bytes value) {
        return new PutRequest(key, value);
    }
}
