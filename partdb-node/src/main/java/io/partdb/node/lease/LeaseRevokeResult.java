package io.partdb.node.lease;

public record LeaseRevokeResult(LeaseId leaseId, long modRevision) {
    public LeaseRevokeResult {
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
