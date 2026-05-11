package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface KeyValueOperations {
    Optional<VersionedValue> getLocal(Bytes key);

    CompletionStage<Optional<VersionedValue>> get(Bytes key);

    CompletionStage<Optional<VersionedValue>> get(Bytes key, ReadConsistency consistency);

    ScanCursor<KeyValueEntry> scanLocal(KeyRange range);

    CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range);

    CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range, ReadConsistency consistency);

    CompletionStage<PutResult> put(PutRequest request);

    default CompletionStage<PutResult> put(Bytes key, Bytes value) {
        return put(PutRequest.of(key, value));
    }

    CompletionStage<DeleteResult> delete(Bytes key);

    CompletionStage<WriteBatchResult> writeBatch(WriteBatch batch);
}
