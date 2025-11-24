package io.partdb.common.statemachine;

public record GrantLease(long leaseId, long ttlMillis, long grantedAtMillis) implements Operation {
    public GrantLease {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be positive");
        }
        if (grantedAtMillis < 0) {
            throw new IllegalArgumentException("grantedAtMillis must be non-negative");
        }
    }
}
