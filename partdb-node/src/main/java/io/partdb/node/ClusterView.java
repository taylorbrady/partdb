package io.partdb.node;

import io.partdb.raft.RaftMembership;
import io.partdb.raft.runtime.RaftStatus;

public interface ClusterView {
    RaftStatus status();

    RaftMembership membership();
}
