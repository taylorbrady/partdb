package io.partdb.raft.rpc;

import io.partdb.raft.LogEntry;

import java.util.List;
import java.util.Objects;

public record AppendEntriesRequest(
    long term,
    String leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit
) {
    public AppendEntriesRequest {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        if (prevLogIndex < 0) {
            throw new IllegalArgumentException("prevLogIndex must be non-negative");
        }
        if (prevLogTerm < 0) {
            throw new IllegalArgumentException("prevLogTerm must be non-negative");
        }
        Objects.requireNonNull(entries, "entries must not be null");
        if (leaderCommit < 0) {
            throw new IllegalArgumentException("leaderCommit must be non-negative");
        }
        entries = List.copyOf(entries);
    }
}
