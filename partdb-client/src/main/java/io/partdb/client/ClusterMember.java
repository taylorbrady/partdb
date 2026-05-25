package io.partdb.client;

import java.util.Objects;

public record ClusterMember(
    String nodeId,
    ClusterMemberRole role,
    boolean leader,
    boolean self
) {
    public ClusterMember {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
    }
}
