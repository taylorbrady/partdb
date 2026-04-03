package io.partdb.consensus;

import io.partdb.raft.RaftRole;

public enum ConsensusRole {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER;

    static ConsensusRole fromRaftRole(RaftRole role) {
        return switch (role) {
            case FOLLOWER -> FOLLOWER;
            case PRE_CANDIDATE -> PRE_CANDIDATE;
            case CANDIDATE -> CANDIDATE;
            case LEADER -> LEADER;
        };
    }
}
