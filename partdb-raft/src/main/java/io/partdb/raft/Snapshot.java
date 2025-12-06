package io.partdb.raft;

public record Snapshot(long index, long term, Membership membership, byte[] data) {

    public Snapshot {
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        if (membership == null) {
            throw new IllegalArgumentException("membership must not be null");
        }
    }
}
