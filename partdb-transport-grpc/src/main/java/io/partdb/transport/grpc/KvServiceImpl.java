package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.partdb.bytes.Bytes;
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
import io.partdb.node.PartDbNode;
import io.partdb.node.PartDbException;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValueEntry;
import io.partdb.node.kv.PutResult;
import io.partdb.node.kv.ScanCursor;
import io.partdb.node.kv.VersionedValue;
import io.partdb.node.lease.LeaseId;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
        long leaseId = request.getLeaseId();

        CompletableFuture<PutResult> future = (leaseId == 0
            ? node.keyValues().put(key, value)
            : node.keyValues().put(key, value, LeaseId.of(leaseId)))
            .toCompletableFuture();

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
    public void batchGet(BatchGetRequest request, StreamObserver<BatchGetResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        CompletableFuture<List<KeyValue>> future = switch (request.getConsistency()) {
            case STALE -> CompletableFuture.completedFuture(readBatchLocal(request));
            case LINEARIZABLE -> readBatchLinearizable(request);
            case UNRECOGNIZED -> CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown read consistency: " + request.getConsistency())
            );
        };

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((results, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(BatchGetResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    BatchGetResponse.Builder builder = BatchGetResponse.newBuilder()
                        .setError(okError());
                    builder.addAllValues(results);
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
                    yield put.getLeaseId() == 0
                        ? node.keyValues().put(toBytes(put.getKey()), toBytes(put.getValue()))
                            .thenApply(PutResult::modRevision)
                            .toCompletableFuture()
                        : node.keyValues().put(
                            toBytes(put.getKey()),
                            toBytes(put.getValue()),
                            LeaseId.of(put.getLeaseId())
                        ).thenApply(PutResult::modRevision).toCompletableFuture();
                }
                case DELETE -> {
                    KvProto.DeleteOp del = writeOp.getDelete();
                    yield node.keyValues().delete(toBytes(del.getKey()))
                        .thenApply(result -> result.modRevision())
                        .toCompletableFuture();
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
                    long revision = futures.stream()
                        .map(CompletableFuture::join)
                        .max(Long::compare)
                        .orElse(0L);
                    responseObserver.onNext(BatchWriteResponse.newBuilder()
                        .setError(okError())
                        .setRevision(revision)
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void grantLease(GrantLeaseRequest request, StreamObserver<GrantLeaseResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        Duration ttl = Duration.ofMillis(request.getTtlMillis());

        node.leases().grant(ttl)
            .toCompletableFuture()
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((grant, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(GrantLeaseResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(GrantLeaseResponse.newBuilder()
                        .setError(okError())
                        .setLeaseId(grant.leaseId().value())
                        .setTtlMillis(grant.ttl().toMillis())
                        .build());
                }
                responseObserver.onCompleted();
            });
    }

    @Override
    public void revokeLease(RevokeLeaseRequest request, StreamObserver<RevokeLeaseResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());

        node.leases().revoke(LeaseId.of(request.getLeaseId()))
            .toCompletableFuture()
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

        node.leases().keepAlive(LeaseId.of(request.getLeaseId()))
            .toCompletableFuture()
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(KeepAliveLeaseResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                } else {
                    responseObserver.onNext(KeepAliveLeaseResponse.newBuilder()
                        .setError(okError())
                        .setTtlMillis(result.ttl().toMillis())
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
            case PartDbException.LeaseNotFound e -> Error.newBuilder()
                .setCode(ErrorCode.NOT_FOUND)
                .setMessage(e.getMessage())
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

    private CompletableFuture<List<KeyValue>> readBatchLinearizable(BatchGetRequest request) {
        List<CompletableFuture<KeyValue>> futures = new ArrayList<>();
        for (ByteString keyBytes : request.getKeysList()) {
            futures.add(node.keyValues()
                .get(Bytes.copyOf(keyBytes.toByteArray()), io.partdb.node.kv.ReadConsistency.LINEARIZABLE)
                .thenApply(value -> buildKeyValue(keyBytes, value))
                .toCompletableFuture());
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(_ -> futures.stream().map(CompletableFuture::join).toList());
    }

    private List<KeyValue> readBatchLocal(BatchGetRequest request) {
        List<KeyValue> results = new ArrayList<>();
        for (ByteString keyBytes : request.getKeysList()) {
            Optional<VersionedValue> value = node.keyValues().getLocal(Bytes.copyOf(keyBytes.toByteArray()));
            results.add(buildKeyValue(keyBytes, value));
        }
        return results;
    }

    private static KeyValue buildKeyValue(ByteString key, Optional<VersionedValue> value) {
        KeyValue.Builder builder = KeyValue.newBuilder()
            .setKey(key)
            .setFound(value.isPresent());
        value.ifPresent(v -> builder
            .setValue(toByteString(v.value()))
            .setRevision(v.modRevision()));
        return builder.build();
    }
}
