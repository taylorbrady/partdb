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
}
