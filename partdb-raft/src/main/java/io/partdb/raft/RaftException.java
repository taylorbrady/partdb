package io.partdb.raft;

import java.util.Optional;

public sealed class RaftException extends RuntimeException
    permits RaftException.NotLeader,
            RaftException.Stopped,
            RaftException.LogCompacted {

    protected RaftException(String message) {
        super(message);
    }

    protected RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class NotLeader extends RaftException {
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

    public static final class Stopped extends RaftException {
        public Stopped() {
            super("Raft node has been stopped");
        }
    }

    public static final class LogCompacted extends RaftException {
        private final long requestedIndex;
        private final long firstAvailableIndex;

        public LogCompacted(long requestedIndex, long firstAvailableIndex) {
            super("Log entry at index " + requestedIndex +
                  " has been compacted (first available: " + firstAvailableIndex + ")");
            this.requestedIndex = requestedIndex;
            this.firstAvailableIndex = firstAvailableIndex;
        }

        public long requestedIndex() {
            return requestedIndex;
        }

        public long firstAvailableIndex() {
            return firstAvailableIndex;
        }
    }
}
