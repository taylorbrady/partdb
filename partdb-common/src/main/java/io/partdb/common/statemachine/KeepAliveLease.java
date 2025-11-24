package io.partdb.common.statemachine;

public record KeepAliveLease(long leaseId) implements Operation {
    public KeepAliveLease {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
    }
}
