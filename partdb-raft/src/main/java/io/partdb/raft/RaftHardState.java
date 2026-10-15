package io.partdb.raft;

import java.util.Objects;
import java.util.Optional;

public record RaftHardState(long term, Optional<String> votedFor, long commit) {
    public static final RaftHardState INITIAL = new RaftHardState(0, Optional.empty(), 0);

    public RaftHardState {
        Validation.nonNegative(term, "term");
        Objects.requireNonNull(votedFor, "votedFor must not be null");
        Validation.nonNegative(commit, "commit");
    }
}
