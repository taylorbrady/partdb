package io.partdb.node.cluster;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record NodeStatus(
    String nodeId,
    NodeRole role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long appliedIndex,
    Optional<Instant> lastLeaderChangeTime,
    boolean running
) {
    public NodeStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        Objects.requireNonNull(lastLeaderChangeTime, "lastLeaderChangeTime must not be null");
    }
}
