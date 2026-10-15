package io.partdb.raft.runtime;

import io.partdb.raft.RaftRole;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record RaftStatus(
    String nodeId,
    RaftRole role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long appliedIndex,
    Optional<Instant> lastLeaderChange,
    boolean running
) {
    public RaftStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        Objects.requireNonNull(lastLeaderChange, "lastLeaderChange must not be null");
    }
}
