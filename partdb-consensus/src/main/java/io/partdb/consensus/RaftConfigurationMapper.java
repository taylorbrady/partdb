package io.partdb.consensus;

import io.partdb.cluster.ClusterMembership;
import io.partdb.raft.RaftConfiguration;

final class RaftConfigurationMapper {
    private RaftConfigurationMapper() {
    }

    static ClusterMembership toClusterMembership(RaftConfiguration configuration) {
        return new ClusterMembership(configuration.voters(), configuration.learners());
    }

    static RaftConfiguration toRaftConfiguration(ClusterMembership membership) {
        return new RaftConfiguration(membership.voters(), membership.learners());
    }
}
