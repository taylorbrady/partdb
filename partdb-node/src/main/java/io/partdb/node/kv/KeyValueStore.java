package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface KeyValueStore {
    Read<Optional<KeyValue>> get(Bytes key, ReadConsistency consistency, Duration timeout);

    Scan scan(KeyRange range, int limit, ReadConsistency consistency, Duration timeout);

    TxnResult transact(Txn txn, Duration timeout);

    long put(Bytes key, Bytes value, Duration timeout);

    long delete(Bytes key, Duration timeout);

    long write(List<Mutation> mutations, Duration timeout);
}
