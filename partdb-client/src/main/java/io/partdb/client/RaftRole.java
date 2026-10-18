package io.partdb.client;

public enum RaftRole {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
