package io.partdb.storage;

public sealed class StorageException extends RuntimeException
    permits StorageException.IO,
            StorageException.Corruption,
            StorageException.Timeout,
            StorageException.Closed {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class IO extends StorageException {
        public IO(String message) {
            super(message);
        }

        public IO(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Corruption extends StorageException {
        public Corruption(String message) {
            super(message);
        }

        public Corruption(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Timeout extends StorageException {
        public Timeout(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Closed extends StorageException {
        public Closed(String message) {
            super(message);
        }
    }
}
