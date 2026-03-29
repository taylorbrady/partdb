package io.partdb.client;

import java.util.Objects;
import java.util.Optional;

public record ClusterStatus(
    String nodeId,
    ClusterNodeRole role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long lastAppliedIndex,
    boolean running
) {
    public ClusterStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leaderId, "leaderId must not be null");
    }
}
