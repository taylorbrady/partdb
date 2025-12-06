package io.partdb.raft;

public record HardState(long term, String votedFor, long commit) {
    public static final HardState INITIAL = new HardState(0, null, 0);

    public HardState {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative: " + term);
        }
        if (commit < 0) {
            throw new IllegalArgumentException("commit must be non-negative: " + commit);
        }
    }
}
