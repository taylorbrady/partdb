package io.partdb.raft;

import java.util.List;

public sealed interface RaftMessage {
    long term();

    sealed interface Request extends RaftMessage {}
    sealed interface Response extends RaftMessage {}

    record AppendEntries(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
    ) implements Request {}

    record AppendEntriesResponse(
        long term,
        boolean success,
        long matchIndex
    ) implements Response {}

    record RequestVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements Request {}

    record RequestVoteResponse(
        long term,
        boolean voteGranted
    ) implements Response {}

    record InstallSnapshot(
        long term,
        String leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        Membership membership,
        byte[] data
    ) implements Request {}

    record InstallSnapshotResponse(long term) implements Response {}

    record PreVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements Request {}

    record PreVoteResponse(
        long term,
        boolean voteGranted
    ) implements Response {}

    record ReadIndex(
        long term,
        byte[] context
    ) implements Request {}

    record ReadIndexResponse(
        long term,
        long readIndex,
        byte[] context
    ) implements Response {}
}
