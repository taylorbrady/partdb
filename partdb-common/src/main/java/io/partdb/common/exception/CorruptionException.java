package io.partdb.common.exception;

public final class CorruptionException extends StorageException {

    public CorruptionException(String message) {
        super(message);
    }

    public CorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
