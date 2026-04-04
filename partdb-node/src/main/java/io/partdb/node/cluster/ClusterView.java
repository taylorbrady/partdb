package io.partdb.node.cluster;

import io.partdb.cluster.ClusterMembership;

public interface ClusterView {
    NodeStatus status();

    ClusterMembership membership();
}
