package io.partdb.raft;

public sealed class RaftException extends RuntimeException
    permits RaftException.LogException,
            RaftException.ElectionException,
            RaftException.ReplicationException,
            RaftException.SnapshotException,
            RaftException.MetadataException {

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class LogException extends RaftException {
        public LogException(String message) {
            super(message);
        }

        public LogException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class ElectionException extends RaftException {
        public ElectionException(String message) {
            super(message);
        }

        public ElectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class ReplicationException extends RaftException {
        public ReplicationException(String message) {
            super(message);
        }

        public ReplicationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class SnapshotException extends RaftException {
        public SnapshotException(String message) {
            super(message);
        }

        public SnapshotException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class MetadataException extends RaftException {
        public MetadataException(String message) {
            super(message);
        }

        public MetadataException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
