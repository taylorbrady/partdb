package io.partdb.raft;

public record HardState(long term, String votedFor) {
    public static final HardState INITIAL = new HardState(0, null);

    public HardState {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative: " + term);
        }
    }
}
