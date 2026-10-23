package io.partdb.storage;

public abstract sealed class StoreException extends RuntimeException {

    private StoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class IO extends StoreException {
        IO(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Corruption extends StoreException {
        Corruption(String message) {
            super(message, null);
        }

        Corruption(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Closed extends StoreException {
        Closed() {
            super("store is closed", null);
        }
    }
}
