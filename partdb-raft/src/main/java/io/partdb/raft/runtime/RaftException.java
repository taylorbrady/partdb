package io.partdb.raft.runtime;

import io.partdb.raft.NodeId;
import io.partdb.raft.Rejection;

import java.util.Optional;

public abstract sealed class RaftException extends RuntimeException {

    private RaftException(String message) {
        super(message);
    }

    private RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    static RaftException of(Rejection rejection) {
        return switch (rejection) {
            case Rejection.NotLeader(var leader) -> new NotLeader(leader);
            case Rejection.TransferInProgress() -> new Busy("a leadership transfer is in progress");
            case Rejection.ChangeInProgress() -> new Busy("a membership change is in progress or the leader has not committed in its term");
            case Rejection.TimedOut() -> new TimedOut();
            case Rejection.Aborted() -> new TransferAborted();
            case Rejection.Invalid(String reason) -> new InvalidRequest(reason);
        };
    }

    public static final class NotLeader extends RaftException {
        private final NodeId leader;

        public NotLeader(Optional<NodeId> leader) {
            super(leader.map(id -> "not the leader; the leader is " + id).orElse("not the leader; the leader is unknown"));
            this.leader = leader.orElse(null);
        }

        public Optional<NodeId> leader() {
            return Optional.ofNullable(leader);
        }
    }

    public static final class Busy extends RaftException {
        public Busy(String reason) {
            super(reason);
        }
    }

    public static final class InvalidRequest extends RaftException {
        public InvalidRequest(String reason) {
            super(reason);
        }
    }

    public static final class TimedOut extends RaftException {
        public TimedOut() {
            super("the leader did not confirm the read in time");
        }
    }

    public static final class TransferAborted extends RaftException {
        public TransferAborted() {
            super("leadership transfer did not complete");
        }
    }

    public static final class OutcomeUnknown extends RaftException {
        private final long index;

        public OutcomeUnknown(long index) {
            super("the outcome of log entry " + index + " is unknown");
            this.index = index;
        }

        public long index() {
            return index;
        }
    }

    public static final class Closed extends RaftException {
        public Closed() {
            super("the raft runtime is closed");
        }
    }

    public static final class Failed extends RaftException {
        public Failed(Throwable cause) {
            super("the raft runtime failed: " + cause.getMessage(), cause);
        }
    }
}
