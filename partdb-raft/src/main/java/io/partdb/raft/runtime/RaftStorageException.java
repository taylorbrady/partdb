package io.partdb.raft.runtime;

sealed class RaftStorageException extends RuntimeException
    permits RaftStorageException.IO, RaftStorageException.Corruption {

    RaftStorageException(String message) {
        super(message);
    }

    RaftStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    static final class IO extends RaftStorageException {
        IO(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static final class Corruption extends RaftStorageException {
        Corruption(String message) {
            super(message);
        }
    }
}
