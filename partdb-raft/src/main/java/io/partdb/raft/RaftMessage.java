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
        List<RaftLogEntry> entries,
        long leaderCommit,
        long round
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
        long matchIndex,
        long round
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
        SnapshotMetadata snapshot
    ) implements Request {
        public InstallSnapshot {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(leaderId, "leaderId must not be null");
            Objects.requireNonNull(snapshot, "snapshot must not be null");
        }
    }

    record InstallSnapshotResponse(long term, long matchIndex) implements Response {
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

    record ReadIndexRequested(
        long term,
        long context
    ) implements Request {
        public ReadIndexRequested {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record ReadIndexResponse(
        long term,
        long readIndex,
        long context
    ) implements Response {
        public ReadIndexResponse {
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }
}
