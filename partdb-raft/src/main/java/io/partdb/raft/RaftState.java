package io.partdb.raft;

public record RaftState(
    HardState hard,
    NodeRole role,
    String leaderId,
    long commitIndex,
    long lastApplied
) {
    public long term() {
        return hard.term();
    }

    public String votedFor() {
        return hard.votedFor();
    }

    public static RaftState fromHard(HardState hard) {
        return new RaftState(hard, NodeRole.FOLLOWER, null, 0, 0);
    }

    public RaftState becomeFollower(long term) {
        return new RaftState(new HardState(term, null), NodeRole.FOLLOWER, null, commitIndex, lastApplied);
    }

    public RaftState becomeCandidate(long term, String votedFor) {
        return new RaftState(new HardState(term, votedFor), NodeRole.CANDIDATE, null, commitIndex, lastApplied);
    }

    public RaftState becomeLeader(long term, String leaderId) {
        return new RaftState(hard, NodeRole.LEADER, leaderId, commitIndex, lastApplied);
    }

    public RaftState withCommitIndex(long newCommitIndex) {
        return new RaftState(hard, role, leaderId, newCommitIndex, lastApplied);
    }

    public RaftState withLastApplied(long newLastApplied) {
        return new RaftState(hard, role, leaderId, commitIndex, newLastApplied);
    }

    public RaftState withVote(String candidate) {
        return new RaftState(new HardState(hard.term(), candidate), role, leaderId, commitIndex, lastApplied);
    }

    public RaftState withLeader(String leader) {
        return new RaftState(hard, role, leader, commitIndex, lastApplied);
    }

    public boolean isLeader() {
        return role == NodeRole.LEADER;
    }

    public boolean isCandidate() {
        return role == NodeRole.CANDIDATE;
    }
}
