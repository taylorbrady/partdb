package io.partdb.node;

import io.partdb.raft.Role;

import java.util.Objects;
import java.util.Optional;

public record NodeStatus(
    String nodeId,
    Role role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long lastAppliedIndex,
    boolean running
) {
    public NodeStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leaderId, "leaderId must not be null");
    }
}
