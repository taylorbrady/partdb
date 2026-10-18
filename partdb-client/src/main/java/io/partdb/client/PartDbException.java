package io.partdb.client;

public abstract sealed class PartDbException extends RuntimeException {

    private PartDbException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class Unavailable extends PartDbException {
        public Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Timeout extends PartDbException {
        public Timeout(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class InvalidRequest extends PartDbException {
        public InvalidRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class OutcomeUnknown extends PartDbException {
        public OutcomeUnknown(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Internal extends PartDbException {
        public Internal(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Cancelled extends PartDbException {
        public Cancelled(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Closed extends PartDbException {
        public Closed() {
            super("the client is closed", null);
        }
    }
}
