package io.partdb.common.statemachine;

public record RevokeLease(long leaseId) implements Operation {
    public RevokeLease {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
    }
}
