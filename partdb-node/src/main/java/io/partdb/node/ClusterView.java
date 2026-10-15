package io.partdb.node;

import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftStatus;

import java.time.Duration;

public interface ClusterView {
    RaftStatus status();

    Membership membership();

    void transferLeadership(NodeId target, Duration timeout);
}
