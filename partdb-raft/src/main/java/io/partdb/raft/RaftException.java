package io.partdb.raft;

import java.util.Optional;

public sealed class RaftException extends RuntimeException
    permits RaftException.NotLeader,
            RaftException.Shutdown,
            RaftException.Compaction,
            RaftException.StorageFailure,
            RaftException.RpcTimeout {

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

    public static final class Shutdown extends RaftException {
        public Shutdown() {
            super("Raft node has been shut down");
        }
    }

    public static final class Compaction extends RaftException {
        private final long requestedIndex;
        private final long firstAvailableIndex;

        public Compaction(long requestedIndex, long firstAvailableIndex) {
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

    public static final class StorageFailure extends RaftException {
        public StorageFailure(String message, Throwable cause) {
            super("Storage operation failed: " + message, cause);
        }
    }

    public static final class RpcTimeout extends RaftException {
        private final String peerId;

        public RpcTimeout(String peerId) {
            super("RPC to peer " + peerId + " timed out");
            this.peerId = peerId;
        }

        public String peerId() {
            return peerId;
        }
    }
}
