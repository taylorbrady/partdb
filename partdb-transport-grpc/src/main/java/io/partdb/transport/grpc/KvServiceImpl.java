package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.partdb.grpc.kv.proto.KvProto;
import io.partdb.grpc.kv.proto.KvProto.BatchGetRequest;
import io.partdb.grpc.kv.proto.KvProto.BatchGetResponse;
import io.partdb.grpc.kv.proto.KvProto.BatchWriteRequest;
import io.partdb.grpc.kv.proto.KvProto.BatchWriteResponse;
import io.partdb.grpc.kv.proto.KvProto.DeleteRequest;
import io.partdb.grpc.kv.proto.KvProto.DeleteResponse;
import io.partdb.grpc.kv.proto.KvProto.Error;
import io.partdb.grpc.kv.proto.KvProto.ErrorCode;
import io.partdb.grpc.kv.proto.KvProto.GetRequest;
import io.partdb.grpc.kv.proto.KvProto.GetResponse;
import io.partdb.grpc.kv.proto.KvProto.GrantLeaseRequest;
import io.partdb.grpc.kv.proto.KvProto.GrantLeaseResponse;
import io.partdb.grpc.kv.proto.KvProto.KeepAliveLeaseRequest;
import io.partdb.grpc.kv.proto.KvProto.KeepAliveLeaseResponse;
import io.partdb.grpc.kv.proto.KvProto.KeyValue;
import io.partdb.grpc.kv.proto.KvProto.PutRequest;
import io.partdb.grpc.kv.proto.KvProto.PutResponse;
import io.partdb.grpc.kv.proto.KvProto.ReadConsistency;
import io.partdb.grpc.kv.proto.KvProto.RequestHeader;
import io.partdb.grpc.kv.proto.KvProto.RevokeLeaseRequest;
import io.partdb.grpc.kv.proto.KvProto.RevokeLeaseResponse;
import io.partdb.grpc.kv.proto.KvProto.ScanRequest;
import io.partdb.grpc.kv.proto.KvProto.ScanResponse;
import io.partdb.grpc.kv.proto.KvServiceGrpc;
import io.partdb.node.KeyValueEntry;
import io.partdb.node.PartDbNode;
import io.partdb.raft.RaftException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

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

        CompletableFuture<Optional<byte[]>> future;
        if (request.getConsistency() == ReadConsistency.STALE) {
            future = CompletableFuture.completedFuture(node.get(toBytes(request.getKey())));
        } else {
            future = CompletableFuture.completedFuture(node.get(toBytes(request.getKey())));
        }

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
                        .setValue(toByteString(result.get()))
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        byte[] key = toBytes(request.getKey());
        byte[] value = toBytes(request.getValue());
        long leaseId = request.getLeaseId();

        node.put(key, value, leaseId)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((_, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(PutResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(PutResponse.newBuilder()
                        .setError(okError())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        byte[] key = toBytes(request.getKey());

        node.delete(key)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((_, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(DeleteResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(DeleteResponse.newBuilder()
                        .setError(okError())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<ScanResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());

        byte[] startKey = request.getStartKey().isEmpty() ? null : toBytes(request.getStartKey());
        byte[] endKey = request.getEndKey().isEmpty() ? null : toBytes(request.getEndKey());
        int limit = request.getLimit() > 0 ? request.getLimit() : Integer.MAX_VALUE;

        CompletableFuture<Stream<KeyValueEntry>> future;
        if (request.getConsistency() == ReadConsistency.STALE) {
            future = CompletableFuture.completedFuture(node.scan(startKey, endKey));
        } else {
            future = CompletableFuture.completedFuture(node.scan(startKey, endKey));
        }

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((stream, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(ScanResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                try (stream) {
                    stream
                        .limit(limit)
                        .forEach(entry -> responseObserver.onNext(ScanResponse.newBuilder()
                            .setError(okError())
                            .setKey(toByteString(entry.key()))
                            .setValue(toByteString(entry.value()))
                            .setRevision(entry.version())
                            .build()));
                } catch (Exception e) {
                    responseObserver.onNext(ScanResponse.newBuilder()
                        .setError(toProtoError(e))
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void batchGet(BatchGetRequest request, StreamObserver<BatchGetResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());

        List<CompletableFuture<KeyValue>> futures = new ArrayList<>();

        for (ByteString keyBytes : request.getKeysList()) {
            Optional<byte[]> value = node.get(keyBytes.toByteArray());
            CompletableFuture<KeyValue> kvFuture = CompletableFuture.completedFuture(buildKeyValue(keyBytes, value));
            futures.add(kvFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((_, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(BatchGetResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    BatchGetResponse.Builder builder = BatchGetResponse.newBuilder()
                        .setError(okError());
                    for (CompletableFuture<KeyValue> f : futures) {
                        builder.addValues(f.join());
                    }
                    responseObserver.onNext(builder.build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void batchWrite(BatchWriteRequest request, StreamObserver<BatchWriteResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());

        List<CompletableFuture<Long>> futures = new ArrayList<>();

        for (KvProto.WriteOp writeOp : request.getOpsList()) {
            CompletableFuture<Long> opFuture = switch (writeOp.getOpCase()) {
                case PUT -> {
                    KvProto.PutOp put = writeOp.getPut();
                    yield node.put(
                        toBytes(put.getKey()),
                        toBytes(put.getValue()),
                        put.getLeaseId()
                    );
                }
                case DELETE -> {
                    KvProto.DeleteOp del = writeOp.getDelete();
                    yield node.delete(toBytes(del.getKey()));
                }
                case OP_NOT_SET -> CompletableFuture.failedFuture(
                    new IllegalArgumentException("WriteOp type not set"));
            };
            futures.add(opFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((_, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(BatchWriteResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(BatchWriteResponse.newBuilder()
                        .setError(okError())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void grantLease(GrantLeaseRequest request, StreamObserver<GrantLeaseResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        long ttlNanos = TimeUnit.MILLISECONDS.toNanos(request.getTtlMillis());

        node.grantLease(ttlNanos)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((leaseId, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(GrantLeaseResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(GrantLeaseResponse.newBuilder()
                        .setError(okError())
                        .setLeaseId(leaseId)
                        .setTtlMillis(request.getTtlMillis())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void revokeLease(RevokeLeaseRequest request, StreamObserver<RevokeLeaseResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        long leaseId = request.getLeaseId();

        node.revokeLease(leaseId)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((_, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(RevokeLeaseResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(RevokeLeaseResponse.newBuilder()
                        .setError(okError())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void keepAliveLease(KeepAliveLeaseRequest request, StreamObserver<KeepAliveLeaseResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        long leaseId = request.getLeaseId();

        node.keepAliveLease(leaseId)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((_, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(KeepAliveLeaseResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(KeepAliveLeaseResponse.newBuilder()
                        .setError(okError())
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

    private static byte[] toBytes(ByteString bytes) {
        return bytes.toByteArray();
    }

    private static ByteString toByteString(byte[] bytes) {
        return ByteString.copyFrom(bytes);
    }

    private static KeyValue buildKeyValue(ByteString key, Optional<byte[]> value) {
        KeyValue.Builder builder = KeyValue.newBuilder()
            .setKey(key)
            .setFound(value.isPresent());
        value.ifPresent(v -> builder.setValue(toByteString(v)));
        return builder.build();
    }

    private static Error okError() {
        return Error.newBuilder()
            .setCode(ErrorCode.OK)
            .build();
    }

    private static Error toProtoError(Throwable ex) {
        Throwable cause = unwrap(ex);

        return switch (cause) {
            case RaftException.NotLeader e -> Error.newBuilder()
                .setCode(ErrorCode.NOT_LEADER)
                .setMessage("Not the leader")
                .setLeaderHint(e.leaderId().orElse(""))
                .build();
            case RaftException.Shutdown _ -> Error.newBuilder()
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
}
