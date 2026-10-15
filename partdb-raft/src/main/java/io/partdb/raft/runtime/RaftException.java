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

        public NotLeader(String leaderId) {
            super(leaderId == null ? "Not leader; leader unknown" : "Not leader; current leader is " + leaderId);
            this.leaderId = leaderId;
        }

        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderId);
        }
    }

    public static final class Closed extends RaftException {
        public Closed() {
            super("Raft runtime is closed");
        }
    }

    public static final class Compacted extends RaftException {
        public Compacted(long requestedIndex, long firstAvailableIndex) {
            super("Log entry " + requestedIndex + " has been compacted; first available is " + firstAvailableIndex);
        }
    }

    public static final class StorageFailure extends RaftException {
        public StorageFailure(String message, Throwable cause) {
            super("Storage operation failed: " + message, cause);
        }
    }

    public static final class RpcTimeout extends RaftException {
        public RpcTimeout(String peerId) {
            super("RPC to peer " + peerId + " timed out");
        }
    }
}
