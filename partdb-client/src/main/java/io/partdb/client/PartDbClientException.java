package io.partdb.client;

public abstract sealed class PartDbClientException extends RuntimeException {

    private PartDbClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class Unavailable extends PartDbClientException {
        public Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Timeout extends PartDbClientException {
        public Timeout(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class InvalidRequest extends PartDbClientException {
        public InvalidRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class OutcomeUnknown extends PartDbClientException {
        public OutcomeUnknown(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Internal extends PartDbClientException {
        public Internal(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Cancelled extends PartDbClientException {
        public Cancelled(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Closed extends PartDbClientException {
        public Closed() {
            super("the client is closed", null);
        }
    }
}
