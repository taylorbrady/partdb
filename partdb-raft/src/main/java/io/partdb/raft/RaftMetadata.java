package io.partdb.raft;

public record RaftMetadata(long currentTerm, String votedFor) {

    public RaftMetadata {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm must be non-negative: " + currentTerm);
        }
    }

    public static RaftMetadata initial() {
        return new RaftMetadata(0, null);
    }

    public RaftMetadata withTerm(long newTerm) {
        return new RaftMetadata(newTerm, votedFor);
    }

    public RaftMetadata withVote(String candidate) {
        return new RaftMetadata(currentTerm, candidate);
    }

    public RaftMetadata clearVote() {
        return new RaftMetadata(currentTerm, null);
    }

    public RaftMetadata incrementTerm() {
        return new RaftMetadata(currentTerm + 1, null);
    }
}
