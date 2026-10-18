package io.partdb.client;

public sealed interface TxnResult {
    record Committed(long revision) implements TxnResult {}

    record ConditionFailed() implements TxnResult {}
}
