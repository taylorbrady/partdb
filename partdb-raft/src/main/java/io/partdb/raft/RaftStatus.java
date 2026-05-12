package io.partdb.raft;

import java.util.Objects;
import java.util.Optional;

public record RaftStatus(
    String nodeId,
    RaftRole role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long lastApplied,
    RaftMembership membership
) {
    public RaftStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        leaderId = Objects.requireNonNull(leaderId, "leaderId must not be null");
        Objects.requireNonNull(membership, "membership must not be null");
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must be non-negative");
        }
        if (lastApplied < 0) {
            throw new IllegalArgumentException("lastApplied must be non-negative");
        }
    }
}
