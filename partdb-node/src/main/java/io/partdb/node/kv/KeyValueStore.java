package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface KeyValueStore {
    CompletionStage<Optional<VersionedValue>> get(Bytes key);

    CompletionStage<Optional<VersionedValue>> get(Bytes key, ReadConsistency consistency);

    CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range);

    CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range, ReadConsistency consistency);

    CompletionStage<WriteResult> put(Bytes key, Bytes value);

    CompletionStage<WriteResult> delete(Bytes key);

    CompletionStage<WriteResult> write(WriteBatch batch);

    CompletionStage<TransactionResult> transact(Transaction transaction);
}
