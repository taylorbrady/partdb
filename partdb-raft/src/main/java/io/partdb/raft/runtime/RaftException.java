package io.partdb.raft.runtime;

import java.util.Optional;

public abstract sealed class RaftException extends RuntimeException {

    private RaftException(String message) {
        super(message);
    }

    private RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class NotLeader extends RaftException {
        private final String leaderId;

        public NotLeader(Optional<String> leaderId) {
            super(leaderId.map(id -> "not leader; current leader is " + id).orElse("not leader; leader unknown"));
            this.leaderId = leaderId.orElse(null);
        }

        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderId);
        }
    }

    public static final class Closed extends RaftException {
        public Closed() {
            super("raft runtime is closed");
        }
    }

    public static final class Failed extends RaftException {
        public Failed(Throwable cause) {
            super("raft runtime failed: " + cause.getMessage(), cause);
        }
    }

    public static final class OutcomeUnknown extends RaftException {
        public OutcomeUnknown(long index) {
            super("outcome of log entry " + index + " is unknown; it was superseded by a snapshot");
        }
    }

    public static final class TransferTimedOut extends RaftException {
        public TransferTimedOut(String target) {
            super("leadership did not transfer to " + target + " in time");
        }
    }
}
