package io.partdb.node;

import io.partdb.raft.Role;

public enum NodeRole {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER;

    static NodeRole fromRaftRole(Role role) {
        return switch (role) {
            case FOLLOWER -> FOLLOWER;
            case PRE_CANDIDATE -> PRE_CANDIDATE;
            case CANDIDATE -> CANDIDATE;
            case LEADER -> LEADER;
        };
    }
}
