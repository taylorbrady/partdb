package io.partdb.node.recovery;

public record RecoveryResult(
    long finalRevision,
    long invalidatedLeaseCount,
    long deletedLeaseAttachedKeyCount
) {
    public RecoveryResult {
        if (finalRevision < 0) {
            throw new IllegalArgumentException("finalRevision must not be negative");
        }
        if (invalidatedLeaseCount < 0) {
            throw new IllegalArgumentException("invalidatedLeaseCount must not be negative");
        }
        if (deletedLeaseAttachedKeyCount < 0) {
            throw new IllegalArgumentException("deletedLeaseAttachedKeyCount must not be negative");
        }
    }
}
