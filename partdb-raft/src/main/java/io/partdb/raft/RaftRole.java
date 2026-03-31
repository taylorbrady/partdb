package io.partdb.raft;

public enum RaftRole {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
