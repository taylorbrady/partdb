package io.partdb.node.kv;

import java.util.Objects;

public record Read<T>(T value, long revision) {
    public Read {
        Objects.requireNonNull(value, "value must not be null");
    }
}
