package io.partdb.raft.runtime;

import io.partdb.raft.Membership;

import java.util.Objects;

public record Bootstrap(ClusterId clusterId, Membership membership) {

    public Bootstrap {
        Objects.requireNonNull(clusterId, "clusterId must not be null");
        Objects.requireNonNull(membership, "membership must not be null");
    }
}
