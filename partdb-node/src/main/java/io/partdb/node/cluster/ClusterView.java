package io.partdb.node.cluster;

import io.partdb.consensus.ConsensusMembership;

public interface ClusterView {
    NodeStatus status();

    ConsensusMembership membership();
}
