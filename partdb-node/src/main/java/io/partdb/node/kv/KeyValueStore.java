package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface KeyValueStore {
    Read<Optional<KeyValue>> get(Bytes key, ReadConsistency consistency, Duration timeout);

    Scan scan(KeyRange range, int limit, ReadConsistency consistency, Duration timeout);

    TxnResult transact(Txn txn, Duration timeout);

    default long put(Bytes key, Bytes value, Duration timeout) {
        return commit(Txn.of(List.of(Mutation.put(key, value))), timeout);
    }

    default long delete(Bytes key, Duration timeout) {
        return commit(Txn.of(List.of(Mutation.delete(key))), timeout);
    }

    default long write(List<Mutation> mutations, Duration timeout) {
        return commit(Txn.of(mutations), timeout);
    }

    private long commit(Txn txn, Duration timeout) {
        return switch (transact(txn, timeout)) {
            case TxnResult.Committed(long revision) -> revision;
            case TxnResult.ConditionFailed() -> throw new IllegalStateException("an unconditional transaction failed a condition");
        };
    }
}
