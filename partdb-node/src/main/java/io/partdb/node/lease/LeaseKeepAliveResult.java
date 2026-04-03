package io.partdb.node.lease;

public record LeaseKeepAliveResult(LeaseId leaseId, long modRevision) {
    public LeaseKeepAliveResult {
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
