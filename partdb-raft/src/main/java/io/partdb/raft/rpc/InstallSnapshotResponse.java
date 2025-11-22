package io.partdb.raft.rpc;

public record InstallSnapshotResponse(
    long term
) {
    public InstallSnapshotResponse {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
    }
}
