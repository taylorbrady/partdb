package io.partdb.server.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.kv.v1.KeyValueServiceGrpc;
import io.partdb.grpc.kv.v1.KvProto;
import io.partdb.grpc.kv.v1.KvProto.DeleteRequest;
import io.partdb.grpc.kv.v1.KvProto.DeleteResponse;
import io.partdb.grpc.kv.v1.KvProto.GetRequest;
import io.partdb.grpc.kv.v1.KvProto.GetResponse;
import io.partdb.grpc.kv.v1.KvProto.PutRequest;
import io.partdb.grpc.kv.v1.KvProto.PutResponse;
import io.partdb.grpc.kv.v1.KvProto.ScanRequest;
import io.partdb.grpc.kv.v1.KvProto.ScanResponse;
import io.partdb.grpc.kv.v1.KvProto.WriteRequest;
import io.partdb.grpc.kv.v1.KvProto.WriteResponse;
import io.partdb.node.Node;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValue;
import io.partdb.node.kv.Mutation;
import io.partdb.node.kv.ReadConsistency;
import io.partdb.node.kv.Scan;

import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
            var read = node.kv().get(bytes(request.getKey()), consistency(request.getConsistency()), timeout);
            var response = GetResponse.newBuilder().setReadRevision(read.revision());
            read.value().ifPresent(entry -> response.setEntry(proto(entry)));
            return response.build();
        });
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responses) {
        Calls.unary(responses, timeout -> PutResponse.newBuilder()
            .setRevision(node.kv().put(bytes(request.getKey()), bytes(request.getValue()), timeout))
            .build());
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responses) {
        Calls.unary(responses, timeout -> DeleteResponse.newBuilder()
            .setRevision(node.kv().delete(bytes(request.getKey()), timeout))
            .build());
    }

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> responses) {
        Calls.unary(responses, timeout -> {
            var mutations = request.getMutationsList().stream().map(KeyValueService::mutation).toList();
            return WriteResponse.newBuilder().setRevision(node.kv().write(mutations, timeout)).build();
        });
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<ScanResponse> responses) {
        var call = (ServerCallStreamObserver<ScanResponse>) responses;
        var lock = new ReentrantLock();
        var ready = lock.newCondition();
        call.setOnReadyHandler(() -> signal(lock, ready));
        call.setOnCancelHandler(() -> signal(lock, ready));
        Scan scan;
        try {
            var range = new KeyRange(bytes(request.getStartKey()), request.hasEndKey() ? Optional.of(bytes(request.getEndKey())) : Optional.empty());
            scan = node.kv().scan(range, request.getLimit(), consistency(request.getConsistency()), Calls.timeout());
        } catch (RuntimeException e) {
            responses.onError(Calls.toStatus(e));
            return;
        }
        try (scan) {
            boolean first = true;
            while (first || scan.hasNext()) {
                var batch = ScanResponse.newBuilder().setReadRevision(scan.readRevision());
                int bytes = 0;
                while (scan.hasNext() && batch.getEntriesCount() < BATCH_ENTRIES && bytes < BATCH_BYTES) {
                    var entry = proto(scan.next());
                    bytes += entry.getSerializedSize();
                    batch.addEntries(entry);
                }
                if (!awaitReady(call, lock, ready)) {
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

    private static boolean awaitReady(ServerCallStreamObserver<ScanResponse> call, ReentrantLock lock, Condition ready) {
        lock.lock();
        try {
            while (!call.isReady() && !call.isCancelled()) {
                ready.awaitUninterruptibly();
            }
            return !call.isCancelled();
        } finally {
            lock.unlock();
        }
    }

    private static void signal(ReentrantLock lock, Condition ready) {
        lock.lock();
        try {
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private static ReadConsistency consistency(KvProto.ReadConsistency consistency) {
        return switch (consistency) {
            case READ_CONSISTENCY_UNSPECIFIED, READ_CONSISTENCY_LINEARIZABLE -> ReadConsistency.LINEARIZABLE;
            case READ_CONSISTENCY_LOCAL -> ReadConsistency.LOCAL;
            case UNRECOGNIZED -> throw new IllegalArgumentException("unknown read consistency");
        };
    }

    private static Mutation mutation(KvProto.Mutation proto) {
        return switch (proto.getKindCase()) {
            case PUT -> Mutation.put(bytes(proto.getPut().getKey()), bytes(proto.getPut().getValue()));
            case DELETE -> Mutation.delete(bytes(proto.getDelete().getKey()));
            case KIND_NOT_SET -> throw new IllegalArgumentException("mutation kind is not set");
        };
    }

    private static KvProto.KeyValue proto(KeyValue entry) {
        return KvProto.KeyValue.newBuilder()
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
