package io.partdb.storage;

public sealed class LSMEngineException extends RuntimeException
    permits LSMEngineException.FlushException,
            LSMEngineException.RecoveryException,
            LSMEngineException.SnapshotException {

    public LSMEngineException(String message) {
        super(message);
    }

    public LSMEngineException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class FlushException extends LSMEngineException {
        public FlushException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class RecoveryException extends LSMEngineException {
        public RecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class SnapshotException extends LSMEngineException {
        public SnapshotException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
