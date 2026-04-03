package io.partdb.node.cluster;

public interface ClusterView {
    NodeStatus status();

    ClusterMembership membership();
}
