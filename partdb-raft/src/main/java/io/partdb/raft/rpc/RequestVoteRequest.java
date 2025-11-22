package io.partdb.raft.rpc;

import java.util.Objects;

public record RequestVoteRequest(
    long term,
    String candidateId,
    long lastLogIndex,
    long lastLogTerm
) {
    public RequestVoteRequest {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        Objects.requireNonNull(candidateId, "candidateId must not be null");
        if (lastLogIndex < 0) {
            throw new IllegalArgumentException("lastLogIndex must be non-negative");
        }
        if (lastLogTerm < 0) {
            throw new IllegalArgumentException("lastLogTerm must be non-negative");
        }
    }
}
