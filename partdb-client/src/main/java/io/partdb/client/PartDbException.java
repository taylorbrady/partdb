package io.partdb.client;

public abstract sealed class PartDbException extends RuntimeException {

    private PartDbException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class Unavailable extends PartDbException {
        Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Timeout extends PartDbException {
        Timeout(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class InvalidRequest extends PartDbException {
        InvalidRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class OutcomeUnknown extends PartDbException {
        OutcomeUnknown(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Internal extends PartDbException {
        Internal(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Cancelled extends PartDbException {
        Cancelled(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Closed extends PartDbException {
        Closed() {
            super("the client is closed", null);
        }
    }
}
