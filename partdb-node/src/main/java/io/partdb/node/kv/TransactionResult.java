package io.partdb.node.kv;

public sealed interface TransactionResult permits TransactionResult.Applied, TransactionResult.ConditionFailed {
    record Applied(long revision) implements TransactionResult {
        public Applied {
            if (revision <= 0) {
                throw new IllegalArgumentException("revision must be positive");
            }
        }
    }

    record ConditionFailed() implements TransactionResult {}
}
