package io.partdb.common;

import java.util.Objects;

public record KVPair(ByteArray key, ByteArray value) {

    public KVPair {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }
}
