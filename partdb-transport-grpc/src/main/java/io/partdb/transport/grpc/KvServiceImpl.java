package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.kv.proto.KvProto;
import io.partdb.grpc.kv.proto.KvProto.BatchWriteRequest;
import io.partdb.grpc.kv.proto.KvProto.BatchWriteResponse;
import io.partdb.grpc.kv.proto.KvProto.DeleteRequest;
import io.partdb.grpc.kv.proto.KvProto.DeleteResponse;
import io.partdb.grpc.kv.proto.KvProto.Error;
import io.partdb.grpc.kv.proto.KvProto.ErrorCode;
import io.partdb.grpc.kv.proto.KvProto.GetRequest;
import io.partdb.grpc.kv.proto.KvProto.GetResponse;
import io.partdb.grpc.kv.proto.KvProto.PutRequest;
import io.partdb.grpc.kv.proto.KvProto.PutResponse;
import io.partdb.grpc.kv.proto.KvProto.RequestHeader;
import io.partdb.grpc.kv.proto.KvProto.ScanRequest;
import io.partdb.grpc.kv.proto.KvProto.ScanResponse;
import io.partdb.grpc.kv.proto.KvServiceGrpc;
import io.partdb.node.PartDbNode;
import io.partdb.node.PartDbException;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValueEntry;
import io.partdb.node.kv.PutResult;
import io.partdb.node.kv.ScanCursor;
import io.partdb.node.kv.VersionedValue;
import io.partdb.node.kv.WriteBatch;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final PartDbNode node;
    private final GrpcServerConfig config;

    KvServiceImpl(PartDbNode node, GrpcServerConfig config) {
        this.node = node;
        this.config = config;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        Bytes key = toBytes(request.getKey());
        CompletableFuture<Optional<VersionedValue>> future = switch (request.getConsistency()) {
            case STALE -> CompletableFuture.completedFuture(node.keyValues().getLocal(key));
            case LINEARIZABLE -> node.keyValues().get(key, io.partdb.node.kv.ReadConsistency.LINEARIZABLE).toCompletableFuture();
            case UNRECOGNIZED -> CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown read consistency: " + request.getConsistency())
            );
        };

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(GetResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else if (result.isEmpty()) {
                    responseObserver.onNext(GetResponse.newBuilder()
                        .setError(Error.newBuilder()
                            .setCode(ErrorCode.NOT_FOUND)
                            .setMessage("Key not found")
                            .build())
                        .build());
                } else {
                    responseObserver.onNext(GetResponse.newBuilder()
                        .setError(okError())
                        .setValue(toByteString(result.get().value()))
                        .setRevision(result.get().modRevision())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        Bytes key = toBytes(request.getKey());
        Bytes value = toBytes(request.getValue());

        CompletableFuture<PutResult> future = node.keyValues().put(key, value).toCompletableFuture();

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(PutResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(PutResponse.newBuilder()
                        .setError(okError())
                        .setRevision(result.modRevision())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        Bytes key = toBytes(request.getKey());

        node.keyValues().delete(key)
            .toCompletableFuture()
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(DeleteResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(DeleteResponse.newBuilder()
                        .setError(okError())
                        .setRevision(result.modRevision())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<ScanResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());

        KeyRange range = toRange(request);
        int limit = request.getLimit() > 0 ? request.getLimit() : Integer.MAX_VALUE;

        CompletableFuture<ScanCursor<KeyValueEntry>> future = switch (request.getConsistency()) {
            case STALE -> CompletableFuture.completedFuture(node.keyValues().scanLocal(range));
            case LINEARIZABLE -> node.keyValues().scan(range, io.partdb.node.kv.ReadConsistency.LINEARIZABLE).toCompletableFuture();
            case UNRECOGNIZED -> CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown read consistency: " + request.getConsistency())
            );
        };

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((cursor, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(ScanResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                try (cursor) {
                    int count = 0;
                    while (count < limit && cursor.hasNext()) {
                        KeyValueEntry entry = cursor.next();
                        responseObserver.onNext(ScanResponse.newBuilder()
                            .setError(okError())
                            .setKey(toByteString(entry.key()))
                            .setValue(toByteString(entry.value()))
                            .setRevision(entry.modRevision())
                            .build());
                        count++;
                    }
                } catch (Exception e) {
                    responseObserver.onNext(ScanResponse.newBuilder()
                        .setError(toProtoError(e))
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void batchWrite(BatchWriteRequest request, StreamObserver<BatchWriteResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        CompletableFuture<Long> future;
        try {
            future = node.keyValues()
                .writeBatch(toWriteBatch(request))
                .thenApply(result -> result.modRevision())
                .toCompletableFuture();
        } catch (RuntimeException e) {
            future = CompletableFuture.failedFuture(e);
        }

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((revision, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(BatchWriteResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(BatchWriteResponse.newBuilder()
                        .setError(okError())
                        .setRevision(revision)
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    private Duration resolveTimeout(RequestHeader header) {
        if (header.getTimeoutMs() <= 0) {
            return config.defaultTimeout();
        }
        return Duration.ofMillis(header.getTimeoutMs());
    }

    private static Bytes toBytes(ByteString bytes) {
        return Bytes.copyOf(bytes.toByteArray());
    }

    private static ByteString toByteString(Bytes bytes) {
        return ByteString.copyFrom(bytes.asReadOnlyByteBuffer());
    }

    private static Error okError() {
        return Error.newBuilder()
            .setCode(ErrorCode.OK)
            .build();
    }

    private static Error toProtoError(Throwable ex) {
        Throwable cause = unwrap(ex);

        return switch (cause) {
            case PartDbException.NotLeader e -> Error.newBuilder()
                .setCode(ErrorCode.NOT_LEADER)
                .setMessage("Not the leader")
                .setLeaderHint(e.leaderId().orElse(""))
                .build();
            case PartDbException.NodeClosed _ -> Error.newBuilder()
                .setCode(ErrorCode.INTERNAL_ERROR)
                .setMessage("Server shutting down")
                .build();
            case TimeoutException _ -> Error.newBuilder()
                .setCode(ErrorCode.INTERNAL_ERROR)
                .setMessage("Request timed out")
                .build();
            case IllegalArgumentException e -> Error.newBuilder()
                .setCode(ErrorCode.INTERNAL_ERROR)
                .setMessage(e.getMessage() != null ? e.getMessage() : "Invalid argument")
                .build();
            default -> Error.newBuilder()
                .setCode(ErrorCode.INTERNAL_ERROR)
                .setMessage(cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName())
                .build();
        };
    }

    private static Throwable unwrap(Throwable ex) {
        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
            return ex.getCause() != null ? ex.getCause() : ex;
        }
        return ex;
    }

    private static KeyRange toRange(ScanRequest request) {
        Optional<Bytes> startKey = request.getStartKey().isEmpty()
            ? Optional.empty()
            : Optional.of(toBytes(request.getStartKey()));
        Optional<Bytes> endKey = request.getEndKey().isEmpty()
            ? Optional.empty()
            : Optional.of(toBytes(request.getEndKey()));
        return new KeyRange(startKey, endKey);
    }

    private static WriteBatch toWriteBatch(BatchWriteRequest request) {
        var builder = WriteBatch.builder();
        for (KvProto.WriteOp writeOp : request.getOpsList()) {
            switch (writeOp.getOpCase()) {
                case PUT -> builder.put(toBytes(writeOp.getPut().getKey()), toBytes(writeOp.getPut().getValue()));
                case DELETE -> builder.delete(toBytes(writeOp.getDelete().getKey()));
                case OP_NOT_SET -> throw new IllegalArgumentException("WriteOp type not set");
            }
        }
        return builder.build();
    }
}
