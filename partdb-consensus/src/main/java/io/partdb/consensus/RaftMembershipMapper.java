package io.partdb.consensus;

import io.partdb.cluster.ClusterMembership;
import io.partdb.raft.RaftMembership;

public final class RaftMembershipMapper {
    private RaftMembershipMapper() {
    }

    static ClusterMembership toClusterMembership(RaftMembership membership) {
        return new ClusterMembership(membership.voters(), membership.learners());
    }

    static RaftMembership toRaftMembership(ClusterMembership membership) {
        return new RaftMembership(membership.voters(), membership.learners());
    }
}
