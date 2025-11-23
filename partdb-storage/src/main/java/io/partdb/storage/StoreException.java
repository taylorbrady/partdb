package io.partdb.storage;

public sealed class StoreException extends RuntimeException
    permits StoreException.FlushException,
            StoreException.RecoveryException,
            StoreException.SnapshotException {

    public StoreException(String message) {
        super(message);
    }

    public StoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class FlushException extends StoreException {
        public FlushException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class RecoveryException extends StoreException {
        public RecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class SnapshotException extends StoreException {
        public SnapshotException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
