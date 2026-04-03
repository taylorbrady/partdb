package io.partdb.node.internal.command;

public sealed interface PartDbCommandResult
    permits PartDbCommandResult.AppliedCommandResult,
            PartDbCommandResult.RejectedCommandResult {

    sealed interface AppliedCommandResult extends PartDbCommandResult
        permits PartDbCommandResult.PutApplied,
                PartDbCommandResult.DeleteApplied,
                PartDbCommandResult.LeaseGranted,
                PartDbCommandResult.LeaseKeptAlive,
                PartDbCommandResult.LeaseRevoked {
    }

    sealed interface RejectedCommandResult extends PartDbCommandResult
        permits PartDbCommandResult.LeaseNotFound {
    }

    record PutApplied(long modRevision) implements AppliedCommandResult {
        public PutApplied {
            if (modRevision <= 0) {
                throw new IllegalArgumentException("modRevision must be positive");
            }
        }
    }

    record DeleteApplied(long modRevision) implements AppliedCommandResult {
        public DeleteApplied {
            if (modRevision <= 0) {
                throw new IllegalArgumentException("modRevision must be positive");
            }
        }
    }

    record LeaseGranted(long modRevision, long leaseId, long ttlNanos) implements AppliedCommandResult {
        public LeaseGranted {
            if (modRevision <= 0) {
                throw new IllegalArgumentException("modRevision must be positive");
            }
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
            if (ttlNanos <= 0) {
                throw new IllegalArgumentException("ttlNanos must be positive");
            }
        }
    }

    record LeaseKeptAlive(long modRevision, long leaseId, long ttlNanos) implements AppliedCommandResult {
        public LeaseKeptAlive {
            if (modRevision <= 0) {
                throw new IllegalArgumentException("modRevision must be positive");
            }
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
            if (ttlNanos <= 0) {
                throw new IllegalArgumentException("ttlNanos must be positive");
            }
        }
    }

    record LeaseRevoked(long modRevision, long leaseId, long deletedKeyCount) implements AppliedCommandResult {
        public LeaseRevoked {
            if (modRevision <= 0) {
                throw new IllegalArgumentException("modRevision must be positive");
            }
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
            if (deletedKeyCount < 0) {
                throw new IllegalArgumentException("deletedKeyCount must not be negative");
            }
        }
    }

    record LeaseNotFound(long leaseId) implements RejectedCommandResult {
        public LeaseNotFound {
            if (leaseId <= 0) {
                throw new IllegalArgumentException("leaseId must be positive");
            }
        }
    }
}
