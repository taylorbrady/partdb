package io.partdb.raft.rpc;

import java.util.Objects;

public record InstallSnapshotRequest(
    long term,
    String leaderId,
    long lastIncludedIndex,
    long lastIncludedTerm,
    byte[] data,
    long checksum
) {
    public InstallSnapshotRequest {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        if (lastIncludedIndex < 0) {
            throw new IllegalArgumentException("lastIncludedIndex must be non-negative");
        }
        if (lastIncludedTerm < 0) {
            throw new IllegalArgumentException("lastIncludedTerm must be non-negative");
        }
        Objects.requireNonNull(data, "data must not be null");
    }
}
