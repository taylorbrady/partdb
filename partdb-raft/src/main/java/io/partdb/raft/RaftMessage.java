package io.partdb.raft;

import java.util.List;
import java.util.Objects;

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
    ) implements Request {
        public AppendEntries {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(leaderId, "leaderId must not be null");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
        }
    }

    record AppendEntriesResponse(
        long term,
        boolean success,
        long matchIndex
    ) implements Response {
        public AppendEntriesResponse {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record RequestVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements Request {
        public RequestVote {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(candidateId, "candidateId must not be null");
        }
    }

    record RequestVoteResponse(
        long term,
        boolean voteGranted
    ) implements Response {
        public RequestVoteResponse {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record InstallSnapshot(
        long term,
        String leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        RaftMembership membership,
        byte[] data
    ) implements Request {
        public InstallSnapshot {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(leaderId, "leaderId must not be null");
            Objects.requireNonNull(membership, "membership must not be null");
            Objects.requireNonNull(data, "data must not be null");
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    record InstallSnapshotResponse(long term) implements Response {
        public InstallSnapshotResponse {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record PreVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements Request {
        public PreVote {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(candidateId, "candidateId must not be null");
        }
    }

    record PreVoteResponse(
        long term,
        boolean voteGranted
    ) implements Response {
        public PreVoteResponse {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record ReadIndex(
        long term,
        byte[] context
    ) implements Request {
        public ReadIndex {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(context, "context must not be null");
            context = context.clone();
        }

        @Override
        public byte[] context() {
            return context.clone();
        }
    }

    record ReadIndexResponse(
        long term,
        long readIndex,
        byte[] context
    ) implements Response {
        public ReadIndexResponse {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(context, "context must not be null");
            context = context.clone();
        }

        @Override
        public byte[] context() {
            return context.clone();
        }
    }
}
