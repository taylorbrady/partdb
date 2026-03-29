package io.partdb.client;

public final class ClusterClientException extends RuntimeException {

    public ClusterClientException(String message) {
        super(message);
    }

    public ClusterClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
