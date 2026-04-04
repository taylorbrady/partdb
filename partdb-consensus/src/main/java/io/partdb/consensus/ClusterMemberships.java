package io.partdb.consensus;

import io.partdb.cluster.ClusterMembership;
import io.partdb.raft.RaftMembership;

final class ClusterMemberships {
    private ClusterMemberships() {
    }

    static ClusterMembership fromRaftMembership(RaftMembership membership) {
        return new ClusterMembership(membership.voters(), membership.learners());
    }

    static RaftMembership toRaftMembership(ClusterMembership membership) {
        return new RaftMembership(membership.voters(), membership.learners());
    }
}
