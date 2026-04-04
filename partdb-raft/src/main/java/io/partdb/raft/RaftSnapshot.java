package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record RaftSnapshot(long index, long term, RaftConfiguration configuration, Bytes data) {

    public RaftSnapshot {
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        if (configuration == null) {
            throw new IllegalArgumentException("configuration must not be null");
        }
        data = Objects.requireNonNull(data, "data must not be null");
    }
}
