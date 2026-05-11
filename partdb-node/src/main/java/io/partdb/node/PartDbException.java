package io.partdb.node;

import java.util.Objects;
import java.util.Optional;

public sealed class PartDbException extends RuntimeException
    permits PartDbException.NotLeader,
            PartDbException.NodeClosed,
            PartDbException.StorageFailure,
            PartDbException.RecoveryFailure {

    protected PartDbException(String message) {
        super(message);
    }

    protected PartDbException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class NotLeader extends PartDbException {
        private final String leaderId;

        public NotLeader(String leaderId) {
            super(leaderId != null
                ? "Not leader; current leader is " + leaderId
                : "Not leader; leader unknown");
            this.leaderId = leaderId;
        }

        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderId);
        }
    }

    public static final class NodeClosed extends PartDbException {
        public NodeClosed() {
            super("Node has been shut down");
        }
    }

    public static final class StorageFailure extends PartDbException {
        public StorageFailure(String message, Throwable cause) {
            super(Objects.requireNonNull(message, "message must not be null"), cause);
        }
    }

    public static final class RecoveryFailure extends PartDbException {
        public RecoveryFailure(String message, Throwable cause) {
            super(Objects.requireNonNull(message, "message must not be null"), cause);
        }
    }
}
