package io.partdb.raft;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

public sealed interface Message {
    long term();

    record PreVote(long term, long lastLogIndex, long lastLogTerm) implements Message {
        public PreVote {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(lastLogIndex, "lastLogIndex");
            Validation.nonNegative(lastLogTerm, "lastLogTerm");
        }
    }

    record PreVoteResponse(long term, boolean granted) implements Message {
        public PreVoteResponse {
            Validation.nonNegative(term, "term");
        }
    }

    record RequestVote(long term, long lastLogIndex, long lastLogTerm, boolean leadershipTransfer) implements Message {
        public RequestVote {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(lastLogIndex, "lastLogIndex");
            Validation.nonNegative(lastLogTerm, "lastLogTerm");
        }
    }

    record RequestVoteResponse(long term, boolean granted) implements Message {
        public RequestVoteResponse {
            Validation.nonNegative(term, "term");
        }
    }

    record AppendEntries(long term, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long commitIndex)
        implements Message {
        public AppendEntries {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(prevLogIndex, "prevLogIndex");
            Validation.nonNegative(prevLogTerm, "prevLogTerm");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            Validation.nonNegative(commitIndex, "commitIndex");
        }
    }

    record AppendAccepted(long term, long matchIndex) implements Message {
        public AppendAccepted {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(matchIndex, "matchIndex");
        }
    }

    record AppendRejected(long term, long rejectedIndex, long hintIndex, long hintTerm) implements Message {
        public AppendRejected {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(rejectedIndex, "rejectedIndex");
            Validation.nonNegative(hintIndex, "hintIndex");
            Validation.nonNegative(hintTerm, "hintTerm");
        }
    }

    record Heartbeat(long term, long commitIndex, long round) implements Message {
        public Heartbeat {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(commitIndex, "commitIndex");
            Validation.nonNegative(round, "round");
        }
    }

    record HeartbeatResponse(long term, long round) implements Message {
        public HeartbeatResponse {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(round, "round");
        }
    }

    record SnapshotInstalled(long term, long matchIndex) implements Message {
        public SnapshotInstalled {
            Validation.nonNegative(term, "term");
            Validation.nonNegative(matchIndex, "matchIndex");
        }
    }

    record ReadIndexRequest(long term, long id) implements Message {
        public ReadIndexRequest {
            Validation.nonNegative(term, "term");
        }
    }

    record ReadIndexResponse(long term, long id, OptionalLong readIndex) implements Message {
        public ReadIndexResponse {
            Validation.nonNegative(term, "term");
            Objects.requireNonNull(readIndex, "readIndex must not be null");
        }
    }

    record TimeoutNow(long term) implements Message {
        public TimeoutNow {
            Validation.nonNegative(term, "term");
        }
    }
}
