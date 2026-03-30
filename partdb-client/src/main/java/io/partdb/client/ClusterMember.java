package io.partdb.client;

import java.util.Objects;
import java.util.Optional;

public record ClusterMember(
    String nodeId,
    Optional<String> raftAddress,
    ClusterMemberRole role,
    boolean leader,
    boolean self
) {
    public ClusterMember {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(raftAddress, "raftAddress must not be null");
        Objects.requireNonNull(role, "role must not be null");
    }
}
