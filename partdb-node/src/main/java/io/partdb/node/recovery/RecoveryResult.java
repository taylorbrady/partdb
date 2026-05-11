package io.partdb.node.recovery;

public record RecoveryResult(long finalRevision) {
    public RecoveryResult {
        if (finalRevision < 0) {
            throw new IllegalArgumentException("finalRevision must not be negative");
        }
    }
}
