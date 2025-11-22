package io.partdb.raft;

import io.partdb.common.statemachine.Operation;

import java.util.Objects;

public record LogEntry(
    long index,
    long term,
    Operation command
) {
    public LogEntry {
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        Objects.requireNonNull(command, "command must not be null");
    }
}
