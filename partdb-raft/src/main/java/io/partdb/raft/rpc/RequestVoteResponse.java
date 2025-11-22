package io.partdb.raft.rpc;

public record RequestVoteResponse(
    long term,
    boolean voteGranted
) {
    public RequestVoteResponse {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
    }
}
