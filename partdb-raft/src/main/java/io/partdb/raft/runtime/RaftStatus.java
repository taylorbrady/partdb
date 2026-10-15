package io.partdb.raft.runtime;

import io.partdb.raft.NodeId;
import io.partdb.raft.Role;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record RaftStatus(
    NodeId nodeId,
    Role role,
    long term,
    Optional<NodeId> leader,
    long commitIndex,
    long appliedIndex,
    long snapshotIndex,
    Optional<Instant> lastLeaderChange,
    boolean running
) {
    public RaftStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leader, "leader must not be null");
        Objects.requireNonNull(lastLeaderChange, "lastLeaderChange must not be null");
    }
}
