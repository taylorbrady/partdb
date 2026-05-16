package io.partdb.consensus;

import io.partdb.raft.RaftMembership;

public final class RaftMembershipMapper {
    private RaftMembershipMapper() {
    }

    static ConsensusMembership toConsensusMembership(RaftMembership membership) {
        return new ConsensusMembership(membership.voters(), membership.learners());
    }

    static RaftMembership toRaftMembership(ConsensusMembership membership) {
        return new RaftMembership(membership.voters(), membership.learners());
    }
}
