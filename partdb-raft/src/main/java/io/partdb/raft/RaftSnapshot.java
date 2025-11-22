package io.partdb.raft;

import io.partdb.common.statemachine.StateSnapshot;

import java.util.Objects;

public record RaftSnapshot(
    long lastIncludedIndex,
    long lastIncludedTerm,
    StateSnapshot stateSnapshot
) {
    public RaftSnapshot {
        if (lastIncludedIndex < 0) {
            throw new IllegalArgumentException("lastIncludedIndex must be non-negative");
        }
        if (lastIncludedTerm < 0) {
            throw new IllegalArgumentException("lastIncludedTerm must be non-negative");
        }
        Objects.requireNonNull(stateSnapshot, "stateSnapshot must not be null");
    }
}
