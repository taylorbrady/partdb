package io.partdb.common.statemachine;

import io.partdb.common.ByteArray;

import java.util.Objects;

public record Put(ByteArray key, ByteArray value, long leaseId) implements Operation {
    public Put {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        if (leaseId < 0) {
            throw new IllegalArgumentException("leaseId must be non-negative");
        }
    }
}
