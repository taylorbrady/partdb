package io.partdb.node.transport;

import io.partdb.node.NodeMembership;

import java.util.List;
import java.util.Objects;

public sealed interface ConsensusMessage {
    long term();

    sealed interface Request extends ConsensusMessage {}

    sealed interface Response extends ConsensusMessage {}

    record AppendEntries(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<ConsensusLogEntry> entries,
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
        NodeMembership membership,
        byte[] data
    ) implements Request {
        public InstallSnapshot {
            validateTerm(term);
            leaderId = requireNonBlank(leaderId, "leaderId");
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
        byte[] context
    ) implements Request {
        public ReadIndex {
            validateTerm(term);
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
            validateTerm(term);
            Objects.requireNonNull(context, "context must not be null");
            context = context.clone();
        }

        @Override
        public byte[] context() {
            return context.clone();
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
