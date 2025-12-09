package io.partdb.storage.compaction;

public final class CompactionException extends RuntimeException {

    public CompactionException(String message) {
        super(message);
    }

    public CompactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
