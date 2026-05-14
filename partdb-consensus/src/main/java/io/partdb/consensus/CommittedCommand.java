package io.partdb.consensus;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record CommittedCommand(long index, long term, Bytes payload) {
    public CommittedCommand {
        if (index <= 0) {
            throw new IllegalArgumentException("index must be positive");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative");
        }
        payload = Objects.requireNonNull(payload, "payload must not be null");
    }
}
