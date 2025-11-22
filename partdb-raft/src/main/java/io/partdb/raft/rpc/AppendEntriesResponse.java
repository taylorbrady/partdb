package io.partdb.raft.rpc;

public record AppendEntriesResponse(
    long term,
    boolean success,
    long matchIndex
) {
    public AppendEntriesResponse {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        if (matchIndex < 0) {
            throw new IllegalArgumentException("matchIndex must be non-negative");
        }
    }
}
