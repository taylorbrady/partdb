package io.partdb.raft;

public record RaftPersistentState(long term, String votedFor, long commit) {
    public static final RaftPersistentState INITIAL = new RaftPersistentState(0, null, 0);

    public RaftPersistentState {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative: " + term);
        }
        if (commit < 0) {
            throw new IllegalArgumentException("commit must be non-negative: " + commit);
        }
    }
}
