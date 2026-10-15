package io.partdb.node;

import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftException;

import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public abstract sealed class NodeException extends RuntimeException {

    private NodeException(String message, Throwable cause) {
        super(message, cause);
    }

    static NodeException of(Throwable error) {
        Throwable cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return switch (cause) {
            case NodeException node -> node;
            case IllegalArgumentException invalid -> new InvalidRequest(invalid.getMessage(), invalid);
            case RaftException.NotLeader notLeader -> new NotLeader(notLeader.leader(), notLeader);
            case RaftException.Busy busy -> new Busy(busy.getMessage(), busy);
            case RaftException.InvalidRequest invalid -> new InvalidRequest(invalid.getMessage(), invalid);
            case RaftException.TimedOut timedOut -> new Unavailable(timedOut.getMessage(), timedOut);
            case RaftException.TransferAborted aborted -> new Unavailable(aborted.getMessage(), aborted);
            case RaftException.OutcomeUnknown unknown -> new OutcomeUnknown(unknown.getMessage(), unknown);
            case RaftException.Closed closed -> new Unavailable("the node is shutting down", closed);
            case RaftException.Failed failed -> new Unavailable("the node has stopped after a failure", failed);
            default -> new Internal(cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName(), cause);
        };
    }

    public static final class NotLeader extends NodeException {
        private final NodeId leader;

        public NotLeader(Optional<NodeId> leader, Throwable cause) {
            super(leader.map(id -> "not the leader; the leader is " + id).orElse("not the leader; the leader is unknown"), cause);
            this.leader = leader.orElse(null);
        }

        public Optional<NodeId> leader() {
            return Optional.ofNullable(leader);
        }
    }

    public static final class Busy extends NodeException {
        public Busy(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Unavailable extends NodeException {
        public Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class InvalidRequest extends NodeException {
        public InvalidRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Timeout extends NodeException {
        public Timeout(String message) {
            super(message, null);
        }
    }

    public static final class OutcomeUnknown extends NodeException {
        public OutcomeUnknown(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Cancelled extends NodeException {
        public Cancelled(String message) {
            super(message, null);
        }
    }

    public static final class Internal extends NodeException {
        public Internal(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
