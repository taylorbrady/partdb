package io.partdb.node;

import io.partdb.raft.Membership;
import io.partdb.raft.runtime.RaftStatus;

public interface ClusterView {
    RaftStatus status();

    Membership membership();
}
