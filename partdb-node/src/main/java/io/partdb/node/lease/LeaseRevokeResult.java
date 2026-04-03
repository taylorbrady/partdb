package io.partdb.node.lease;

import java.util.Objects;

public record LeaseRevokeResult(LeaseId leaseId, long modRevision, long deletedKeyCount) {
    public LeaseRevokeResult {
        leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
        if (deletedKeyCount < 0) {
            throw new IllegalArgumentException("deletedKeyCount must not be negative");
        }
    }
}
