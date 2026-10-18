package io.partdb.server.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.kv.v1.DeleteRequest;
import io.partdb.grpc.kv.v1.DeleteResponse;
import io.partdb.grpc.kv.v1.GetRequest;
import io.partdb.grpc.kv.v1.GetResponse;
import io.partdb.grpc.kv.v1.KeyValueServiceGrpc;
import io.partdb.grpc.kv.v1.PutRequest;
import io.partdb.grpc.kv.v1.PutResponse;
import io.partdb.grpc.kv.v1.ScanRequest;
import io.partdb.grpc.kv.v1.ScanResponse;
import io.partdb.grpc.kv.v1.TxnRequest;
import io.partdb.grpc.kv.v1.TxnResponse;
import io.partdb.node.Condition;
import io.partdb.node.Node;
import io.partdb.node.ReadConsistency;
import io.partdb.node.Txn;
import io.partdb.node.TxnResult;
import io.partdb.storage.Cursor;
import io.partdb.storage.KeyRange;
import io.partdb.storage.KeyValue;
import io.partdb.storage.Mutation;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

final class KeyValueService extends KeyValueServiceGrpc.KeyValueServiceImplBase {

    private static final int BATCH_ENTRIES = 256;
    private static final int BATCH_BYTES = 1 << 20;

    private final Node node;

    KeyValueService(Node node) {
        this.node = node;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responses) {
        Calls.unary(responses, timeout -> {
            var read = node.get(bytes(request.getKey()), consistency(request.getConsistency()), timeout);
            var response = GetResponse.newBuilder().setReadRevision(read.revision());
            read.value().ifPresent(entry -> response.setEntry(proto(entry)));
            return response.build();
        });
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responses) {
        Calls.unary(responses, timeout -> PutResponse.newBuilder()
            .setRevision(node.write(List.of(Mutation.put(bytes(request.getKey()), bytes(request.getValue()))), timeout))
            .build());
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responses) {
        Calls.unary(responses, timeout -> DeleteResponse.newBuilder()
            .setRevision(node.write(List.of(Mutation.delete(bytes(request.getKey()))), timeout))
            .build());
    }

    @Override
    public void txn(TxnRequest request, StreamObserver<TxnResponse> responses) {
        Calls.unary(responses, timeout -> {
            var txn = decoded(() -> new Txn(
                request.getConditionsList().stream().map(KeyValueService::condition).toList(),
                request.getMutationsList().stream().map(KeyValueService::mutation).toList()));
            var response = TxnResponse.newBuilder();
            switch (node.transact(txn, timeout)) {
                case TxnResult.Committed(long revision) -> response.setCommitted(TxnResponse.Committed.newBuilder().setRevision(revision));
                case TxnResult.ConditionFailed() -> response.setConditionFailed(TxnResponse.ConditionFailed.getDefaultInstance());
            }
            return response.build();
        });
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<ScanResponse> responses) {
        var call = (ServerCallStreamObserver<ScanResponse>) responses;
        var flow = new FlowControl(call);
        Cursor cursor;
        try {
            if (request.getLimit() < 0) {
                throw Calls.invalid("limit must not be negative");
            }
            cursor = node.scan(range(request), consistency(request.getConsistency()), Calls.timeout());
        } catch (RuntimeException e) {
            responses.onError(Calls.toStatus(e));
            return;
        }
        int remaining = request.getLimit() == 0 ? Integer.MAX_VALUE : request.getLimit();
        try (cursor) {
            boolean first = true;
            while (first || (remaining > 0 && cursor.hasNext())) {
                var batch = ScanResponse.newBuilder().setReadRevision(cursor.revision());
                int bytes = 0;
                while (remaining > 0 && cursor.hasNext() && batch.getEntriesCount() < BATCH_ENTRIES && bytes < BATCH_BYTES) {
                    var entry = proto(cursor.next());
                    bytes += entry.getSerializedSize();
                    batch.addEntries(entry);
                    remaining--;
                }
                if (!flow.awaitReady()) {
                    return;
                }
                call.onNext(batch.build());
                first = false;
            }
            call.onCompleted();
        } catch (RuntimeException e) {
            if (!call.isCancelled()) {
                call.onError(Calls.toStatus(e));
            }
        }
    }

    private static KeyRange range(ScanRequest request) {
        var end = request.hasEndKey() ? Optional.of(bytes(request.getEndKey())) : Optional.<Bytes>empty();
        return decoded(() -> new KeyRange(bytes(request.getStartKey()), end));
    }

    private static <T> T decoded(Supplier<T> value) {
        try {
            return value.get();
        } catch (IllegalArgumentException e) {
            throw Calls.invalid(e.getMessage());
        }
    }

    private static ReadConsistency consistency(io.partdb.grpc.kv.v1.ReadConsistency consistency) {
        return switch (consistency) {
            case READ_CONSISTENCY_UNSPECIFIED, READ_CONSISTENCY_LINEARIZABLE -> ReadConsistency.LINEARIZABLE;
            case READ_CONSISTENCY_LOCAL -> ReadConsistency.LOCAL;
            case UNRECOGNIZED -> throw Calls.invalid("unknown read consistency");
        };
    }

    private static Condition condition(io.partdb.grpc.kv.v1.Condition proto) {
        return switch (proto.getKindCase()) {
            case EXISTS -> Condition.exists(bytes(proto.getExists().getKey()));
            case MISSING -> Condition.missing(bytes(proto.getMissing().getKey()));
            case VALUE_EQUALS -> Condition.valueEquals(bytes(proto.getValueEquals().getKey()), bytes(proto.getValueEquals().getValue()));
            case REVISION_EQUALS -> Condition.revisionEquals(bytes(proto.getRevisionEquals().getKey()), proto.getRevisionEquals().getRevision());
            case KIND_NOT_SET -> throw Calls.invalid("condition kind is not set");
        };
    }

    private static Mutation mutation(io.partdb.grpc.kv.v1.Mutation proto) {
        return switch (proto.getKindCase()) {
            case PUT -> Mutation.put(bytes(proto.getPut().getKey()), bytes(proto.getPut().getValue()));
            case DELETE -> Mutation.delete(bytes(proto.getDelete().getKey()));
            case KIND_NOT_SET -> throw Calls.invalid("mutation kind is not set");
        };
    }

    private static io.partdb.grpc.kv.v1.KeyValue proto(KeyValue entry) {
        return io.partdb.grpc.kv.v1.KeyValue.newBuilder()
            .setKey(bytes(entry.key()))
            .setValue(bytes(entry.value()))
            .setRevision(entry.revision())
            .build();
    }

    private static Bytes bytes(ByteString value) {
        return Bytes.copyOf(value.asReadOnlyByteBuffer());
    }

    private static ByteString bytes(Bytes value) {
        return UnsafeByteOperations.unsafeWrap(value.asReadOnlyByteBuffer());
    }
}
