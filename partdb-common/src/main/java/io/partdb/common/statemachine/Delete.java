package io.partdb.common.statemachine;

import io.partdb.common.ByteArray;

import java.util.Objects;

public record Delete(ByteArray key) implements Operation {
    public Delete {
        Objects.requireNonNull(key, "key must not be null");
    }
}
