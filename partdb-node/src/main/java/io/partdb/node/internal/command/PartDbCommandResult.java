package io.partdb.node.internal.command;

public sealed interface PartDbCommandResult
    permits PartDbCommandResult.AppliedCommandResult {

    sealed interface AppliedCommandResult extends PartDbCommandResult
        permits PartDbCommandResult.PutApplied,
                PartDbCommandResult.DeleteApplied,
                PartDbCommandResult.BatchWriteApplied {
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

    record BatchWriteApplied(long modRevision) implements AppliedCommandResult {
        public BatchWriteApplied {
            if (modRevision <= 0) {
                throw new IllegalArgumentException("modRevision must be positive");
            }
        }
    }

}
