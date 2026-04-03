package io.partdb.node.replication;

import io.partdb.bytes.Bytes;
import io.partdb.node.cluster.ClusterMembership;

import java.util.List;
import java.util.Objects;

public sealed interface ReplicationRpc {
    long term();

    sealed interface Request extends ReplicationRpc {}

    sealed interface Response extends ReplicationRpc {}

    record AppendEntries(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<ReplicationLogEntry> entries,
        long leaderCommit
    ) implements Request {
        public AppendEntries {
            validateTerm(term);
            leaderId = requireNonBlank(leaderId, "leaderId");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
        }
    }

    record AppendEntriesResponse(
        long term,
        boolean success,
        long matchIndex
    ) implements Response {
        public AppendEntriesResponse {
            validateTerm(term);
        }
    }

    record RequestVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements Request {
        public RequestVote {
            validateTerm(term);
            candidateId = requireNonBlank(candidateId, "candidateId");
        }
    }

    record RequestVoteResponse(
        long term,
        boolean voteGranted
    ) implements Response {
        public RequestVoteResponse {
            validateTerm(term);
        }
    }

    record InstallSnapshot(
        long term,
        String leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        ClusterMembership membership,
        Bytes data
    ) implements Request {
        public InstallSnapshot {
            validateTerm(term);
            leaderId = requireNonBlank(leaderId, "leaderId");
            membership = Objects.requireNonNull(membership, "membership must not be null");
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }

    record InstallSnapshotResponse(long term) implements Response {
        public InstallSnapshotResponse {
            validateTerm(term);
        }
    }

    record PreVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements Request {
        public PreVote {
            validateTerm(term);
            candidateId = requireNonBlank(candidateId, "candidateId");
        }
    }

    record PreVoteResponse(
        long term,
        boolean voteGranted
    ) implements Response {
        public PreVoteResponse {
            validateTerm(term);
        }
    }

    record ReadIndex(
        long term,
        Bytes context
    ) implements Request {
        public ReadIndex {
            validateTerm(term);
            context = Objects.requireNonNull(context, "context must not be null");
        }
    }

    record ReadIndexResponse(
        long term,
        long readIndex,
        Bytes context
    ) implements Response {
        public ReadIndexResponse {
            validateTerm(term);
            context = Objects.requireNonNull(context, "context must not be null");
        }
    }

    private static void validateTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
