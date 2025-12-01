package io.partdb.client;

public sealed class KvClientException extends RuntimeException
    permits KvClientException.ConnectionFailed,
            KvClientException.RequestTimeout,
            KvClientException.Conflict,
            KvClientException.LeaseExpired,
            KvClientException.ClusterUnavailable {

    protected KvClientException(String message) {
        super(message);
    }

    protected KvClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public static final class ConnectionFailed extends KvClientException {
        private final String endpoint;

        public ConnectionFailed(String endpoint, Throwable cause) {
            super("Failed to connect to " + endpoint, cause);
            this.endpoint = endpoint;
        }

        public String endpoint() {
            return endpoint;
        }
    }

    public static final class RequestTimeout extends KvClientException {
        public RequestTimeout(String operation) {
            super("Request timed out: " + operation);
        }
    }

    public static final class Conflict extends KvClientException {
        public Conflict(String message) {
            super(message);
        }
    }

    public static final class LeaseExpired extends KvClientException {
        private final long leaseId;

        public LeaseExpired(long leaseId) {
            super("Lease expired: " + leaseId);
            this.leaseId = leaseId;
        }

        public long leaseId() {
            return leaseId;
        }
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
