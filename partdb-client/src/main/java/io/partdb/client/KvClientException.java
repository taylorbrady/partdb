package io.partdb.client;

public sealed class KvClientException extends RuntimeException
    permits KvClientException.ClusterUnavailable {

    protected KvClientException(String message) {
        super(message);
    }

    protected KvClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class ClusterUnavailable extends KvClientException {
        public ClusterUnavailable(String message) {
            super(message);
        }

        public ClusterUnavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
