package io.partdb.raft;

public record RaftNodeState(
    long currentTerm,
    NodeRole role,
    String votedFor,
    String leaderId,
    long commitIndex,
    long lastApplied
) {
    public static RaftNodeState initial() {
        return new RaftNodeState(0, NodeRole.FOLLOWER, null, null, 0, 0);
    }

    public RaftNodeState becomeFollower(long term) {
        return new RaftNodeState(term, NodeRole.FOLLOWER, null, null, commitIndex, lastApplied);
    }

    public RaftNodeState becomeCandidate(long term, String votedFor) {
        return new RaftNodeState(term, NodeRole.CANDIDATE, votedFor, null, commitIndex, lastApplied);
    }

    public RaftNodeState becomeLeader(long term, String leaderId) {
        return new RaftNodeState(term, NodeRole.LEADER, null, leaderId, commitIndex, lastApplied);
    }

    public RaftNodeState withCommitIndex(long newCommitIndex) {
        return new RaftNodeState(currentTerm, role, votedFor, leaderId, newCommitIndex, lastApplied);
    }

    public RaftNodeState withLastApplied(long newLastApplied) {
        return new RaftNodeState(currentTerm, role, votedFor, leaderId, commitIndex, newLastApplied);
    }

    public RaftNodeState withVote(String candidate) {
        return new RaftNodeState(currentTerm, role, candidate, leaderId, commitIndex, lastApplied);
    }

    public RaftNodeState withLeader(String leader) {
        return new RaftNodeState(currentTerm, role, votedFor, leader, commitIndex, lastApplied);
    }

    public boolean isLeader() {
        return role == NodeRole.LEADER;
    }

    public boolean isCandidate() {
        return role == NodeRole.CANDIDATE;
    }
}
