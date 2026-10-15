package io.partdb.raft;

import java.util.Objects;

public record SnapshotMetadata(long index, long term, Membership membership) {
    public SnapshotMetadata {
        Validation.positive(index, "index");
        Validation.positive(term, "term");
        Objects.requireNonNull(membership, "membership must not be null");
    }
}
