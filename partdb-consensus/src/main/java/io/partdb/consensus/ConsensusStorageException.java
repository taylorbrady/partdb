package io.partdb.consensus;

sealed class ConsensusStorageException extends RuntimeException
    permits ConsensusStorageException.IO, ConsensusStorageException.Corruption {

    ConsensusStorageException(String message) {
        super(message);
    }

    ConsensusStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    static final class IO extends ConsensusStorageException {
        IO(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static final class Corruption extends ConsensusStorageException {
        Corruption(String message) {
            super(message);
        }
    }
}
