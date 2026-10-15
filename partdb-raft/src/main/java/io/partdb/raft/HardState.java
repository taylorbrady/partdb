package io.partdb.raft;

import java.util.Objects;
import java.util.Optional;

public record HardState(long term, Optional<NodeId> votedFor, long commit) {
    public static final HardState INITIAL = new HardState(0, Optional.empty(), 0);

    public HardState {
        Validation.nonNegative(term, "term");
        Objects.requireNonNull(votedFor, "votedFor must not be null");
        Validation.nonNegative(commit, "commit");
    }
}
