package io.partdb.common;

import java.util.Objects;

public record KeyValue(ByteArray key, ByteArray value) {

    public KeyValue {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }
}
