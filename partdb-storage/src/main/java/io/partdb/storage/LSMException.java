package io.partdb.storage;

public sealed class LSMException extends RuntimeException
    permits LSMException.FlushException,
            LSMException.RecoveryException,
            LSMException.CheckpointException,
            LSMException.ConcurrencyException {

    public LSMException(String message) {
        super(message);
    }

    public LSMException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class FlushException extends LSMException {
        public FlushException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class RecoveryException extends LSMException {
        public RecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class CheckpointException extends LSMException {
        public CheckpointException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class ConcurrencyException extends LSMException {
        public ConcurrencyException(String message) {
            super(message);
        }
    }
}
