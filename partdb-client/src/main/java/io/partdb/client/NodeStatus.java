package io.partdb.client;

import java.util.Objects;
import java.util.Optional;

public record NodeStatus(
    String nodeId,
    RaftRole role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long appliedIndex,
    long snapshotIndex
) {
    public NodeStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leaderId, "leaderId must not be null");
    }
}
