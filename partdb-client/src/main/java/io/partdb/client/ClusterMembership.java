package io.partdb.client;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record ClusterMembership(
    Optional<String> leaderId,
    List<ClusterMember> members
) {
    public ClusterMembership {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        Objects.requireNonNull(members, "members must not be null");
        members = List.copyOf(members);
    }
}
