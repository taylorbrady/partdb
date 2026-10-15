package io.partdb.node.cluster;

import io.partdb.raft.RaftMembership;

public interface ClusterView {
    NodeStatus status();

    RaftMembership membership();
}
