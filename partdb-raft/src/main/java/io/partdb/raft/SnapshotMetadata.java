package io.partdb.raft;

import java.util.Objects;

public record SnapshotMetadata(long index, long term, RaftMembership membership) {
    public SnapshotMetadata {
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        if (index > 0 && term == 0) {
            throw new IllegalArgumentException("a snapshot of committed entries must have a positive term");
        }
        Objects.requireNonNull(membership, "membership must not be null");
    }
}
