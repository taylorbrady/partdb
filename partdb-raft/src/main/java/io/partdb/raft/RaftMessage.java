package io.partdb.raft;

import java.util.List;
import java.util.Objects;

public sealed interface RaftMessage {
    long term();

    record PreVote(long term, long lastLogIndex, long lastLogTerm) implements RaftMessage {
        public PreVote {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(lastLogIndex, "lastLogIndex");
            Validation.nonNegative(lastLogTerm, "lastLogTerm");
        }
    }

    record PreVoteResponse(long term, boolean granted) implements RaftMessage {
        public PreVoteResponse {
            Validation.nonNegative(term, "term");
        }
    }

    record RequestVote(long term, long lastLogIndex, long lastLogTerm, boolean leadershipTransfer) implements RaftMessage {
        public RequestVote {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(lastLogIndex, "lastLogIndex");
            Validation.nonNegative(lastLogTerm, "lastLogTerm");
        }
    }

    record RequestVoteResponse(long term, boolean granted) implements RaftMessage {
        public RequestVoteResponse {
            Validation.nonNegative(term, "term");
        }
    }

    record AppendEntries(long term, long prevLogIndex, long prevLogTerm, List<RaftLogEntry> entries, long commitIndex)
        implements RaftMessage {
        public AppendEntries {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(prevLogIndex, "prevLogIndex");
            Validation.nonNegative(prevLogTerm, "prevLogTerm");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            Validation.nonNegative(commitIndex, "commitIndex");
        }
    }

    record AppendAccepted(long term, long matchIndex) implements RaftMessage {
        public AppendAccepted {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(matchIndex, "matchIndex");
        }
    }

    record AppendRejected(long term, long rejectedIndex, long hintIndex, long hintTerm) implements RaftMessage {
        public AppendRejected {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(rejectedIndex, "rejectedIndex");
            Validation.nonNegative(hintIndex, "hintIndex");
            Validation.nonNegative(hintTerm, "hintTerm");
        }
    }

    record Heartbeat(long term, long commitIndex, long round) implements RaftMessage {
        public Heartbeat {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(commitIndex, "commitIndex");
            Validation.nonNegative(round, "round");
        }
    }

    record HeartbeatResponse(long term, long round) implements RaftMessage {
        public HeartbeatResponse {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(round, "round");
        }
    }

    record InstallSnapshot(long term, SnapshotMetadata snapshot) implements RaftMessage {
        public InstallSnapshot {
            Validation.nonNegative(term, "term");
            Objects.requireNonNull(snapshot, "snapshot must not be null");
        }
    }

    record InstallSnapshotResponse(long term, long matchIndex) implements RaftMessage {
        public InstallSnapshotResponse {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(matchIndex, "matchIndex");
        }
    }

    record ReadIndex(long term, long id) implements RaftMessage {
        public ReadIndex {
            Validation.nonNegative(term, "term");
        }
    }

    record ReadIndexResponse(long term, long id, long readIndex) implements RaftMessage {
        public ReadIndexResponse {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(readIndex, "readIndex");
        }

        public boolean rejected() {
            return readIndex == 0;
        }
    }

    record TimeoutNow(long term) implements RaftMessage {
        public TimeoutNow {
            Validation.nonNegative(term, "term");
        }
    }
}
