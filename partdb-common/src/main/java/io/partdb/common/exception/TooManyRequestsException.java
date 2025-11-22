package io.partdb.common.exception;

public final class TooManyRequestsException extends RuntimeException {
    private final int queueSize;

    public TooManyRequestsException(int queueSize) {
        super("Too many pending requests: " + queueSize);
        this.queueSize = queueSize;
    }

    public int queueSize() {
        return queueSize;
    }
}
