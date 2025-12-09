package io.partdb.server.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.server.NotLeaderException;
import io.partdb.protocol.kv.proto.KvProto;
import io.partdb.protocol.kv.proto.KvProto.BatchGetRequest;
import io.partdb.protocol.kv.proto.KvProto.BatchGetResponse;
import io.partdb.protocol.kv.proto.KvProto.BatchWriteRequest;
import io.partdb.protocol.kv.proto.KvProto.BatchWriteResponse;
import io.partdb.protocol.kv.proto.KvProto.DeleteRequest;
import io.partdb.protocol.kv.proto.KvProto.DeleteResponse;
import io.partdb.protocol.kv.proto.KvProto.Error;
import io.partdb.protocol.kv.proto.KvProto.ErrorCode;
import io.partdb.protocol.kv.proto.KvProto.GetRequest;
import io.partdb.protocol.kv.proto.KvProto.GetResponse;
import io.partdb.protocol.kv.proto.KvProto.GrantLeaseRequest;
import io.partdb.protocol.kv.proto.KvProto.GrantLeaseResponse;
import io.partdb.protocol.kv.proto.KvProto.KeepAliveLeaseRequest;
import io.partdb.protocol.kv.proto.KvProto.KeepAliveLeaseResponse;
import io.partdb.protocol.kv.proto.KvProto.KeyValue;
import io.partdb.protocol.kv.proto.KvProto.PutRequest;
import io.partdb.protocol.kv.proto.KvProto.PutResponse;
import io.partdb.protocol.kv.proto.KvProto.ReadConsistency;
import io.partdb.protocol.kv.proto.KvProto.RequestHeader;
import io.partdb.protocol.kv.proto.KvProto.RevokeLeaseRequest;
import io.partdb.protocol.kv.proto.KvProto.RevokeLeaseResponse;
import io.partdb.protocol.kv.proto.KvProto.ScanRequest;
import io.partdb.protocol.kv.proto.KvProto.ScanResponse;
import io.partdb.protocol.kv.proto.KvServiceGrpc;
import io.partdb.server.KvStore;
import io.partdb.server.Lessor;
import io.partdb.server.Proposer;

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

public final class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final Proposer proposer;
    private final Lessor lessor;
    private final KvStore kvStore;
    private final KvServerConfig config;

    public KvServiceImpl(Proposer proposer, Lessor lessor, KvStore kvStore, KvServerConfig config) {
        this.proposer = proposer;
        this.lessor = lessor;
        this.kvStore = kvStore;
        this.config = config;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        ByteArray key = toByteArray(request.getKey());

        CompletableFuture<Optional<ByteArray>> future;
        if (request.getConsistency() == ReadConsistency.STALE) {
            future = CompletableFuture.completedFuture(kvStore.get(key));
        } else {
            future = CompletableFuture.completedFuture(kvStore.get(key));
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
        ByteArray key = toByteArray(request.getKey());
        ByteArray value = toByteArray(request.getValue());
        long leaseId = request.getLeaseId();

        proposer.put(key, value, leaseId)
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
        ByteArray key = toByteArray(request.getKey());

        proposer.delete(key)
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

        ByteArray startKey = request.getStartKey().isEmpty() ? null : toByteArray(request.getStartKey());
        ByteArray endKey = request.getEndKey().isEmpty() ? null : toByteArray(request.getEndKey());
        int limit = request.getLimit() > 0 ? request.getLimit() : Integer.MAX_VALUE;

        CompletableFuture<Stream<Entry>> future;
        if (request.getConsistency() == ReadConsistency.STALE) {
            future = CompletableFuture.completedFuture(kvStore.scan(startKey, endKey));
        } else {
            future = CompletableFuture.completedFuture(kvStore.scan(startKey, endKey));
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
            ByteArray key = toByteArray(keyBytes);
            Optional<ByteArray> value = kvStore.get(key);
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

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (KvProto.WriteOp writeOp : request.getOpsList()) {
            CompletableFuture<Void> opFuture = switch (writeOp.getOpCase()) {
                case PUT -> {
                    KvProto.PutOp put = writeOp.getPut();
                    yield proposer.put(
                        toByteArray(put.getKey()),
                        toByteArray(put.getValue()),
                        put.getLeaseId()
                    );
                }
                case DELETE -> {
                    KvProto.DeleteOp del = writeOp.getDelete();
                    yield proposer.delete(toByteArray(del.getKey()));
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

        lessor.grant(ttlNanos)
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

        lessor.revoke(leaseId)
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

        lessor.keepAlive(leaseId)
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

    private static ByteArray toByteArray(ByteString bytes) {
        return ByteArray.copyOf(bytes.toByteArray());
    }

    private static ByteString toByteString(ByteArray bytes) {
        return ByteString.copyFrom(bytes.toByteArray());
    }

    private static KeyValue buildKeyValue(ByteString key, Optional<ByteArray> value) {
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
            case NotLeaderException e -> Error.newBuilder()
                .setCode(ErrorCode.NOT_LEADER)
                .setMessage("Not the leader")
                .setLeaderHint(e.leaderId().orElse(""))
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
