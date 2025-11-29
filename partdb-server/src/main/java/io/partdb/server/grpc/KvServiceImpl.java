package io.partdb.server.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.exception.NotLeaderException;
import io.partdb.common.exception.TooManyRequestsException;
import io.partdb.common.statemachine.Delete;
import io.partdb.common.statemachine.Put;
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
import io.partdb.raft.RaftNode;
import io.partdb.server.Database;
import io.partdb.server.Lessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(KvServiceImpl.class);

    private final RaftNode raftNode;
    private final Database database;
    private final Lessor lessor;
    private final KvServerConfig config;

    public KvServiceImpl(RaftNode raftNode, Database database, Lessor lessor, KvServerConfig config) {
        this.raftNode = raftNode;
        this.database = database;
        this.lessor = lessor;
        this.config = config;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        Duration timeout = resolveTimeout(request.getHeader());
        ByteArray key = toByteArray(request.getKey());

        CompletableFuture<Optional<ByteArray>> future;
        if (request.getConsistency() == ReadConsistency.STALE) {
            future = CompletableFuture.completedFuture(database.get(key));
        } else {
            future = raftNode.get(key);
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

        Put operation = new Put(key, value, leaseId);

        raftNode.propose(operation)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((__, ex) -> {
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

        Delete operation = new Delete(key);

        raftNode.propose(operation)
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((__, ex) -> {
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

        CompletableFuture<Iterator<Entry>> future;
        if (request.getConsistency() == ReadConsistency.STALE) {
            future = CompletableFuture.completedFuture(database.scan(startKey, endKey));
        } else {
            future = raftNode.scan(startKey, endKey);
        }

        future
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((iterator, ex) -> {
                if (ex != null) {
                    responseObserver.onNext(ScanResponse.newBuilder()
                        .setError(toProtoError(ex))
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                try {
                    int count = 0;
                    while (iterator.hasNext() && count < limit) {
                        Entry entry = iterator.next();
                        responseObserver.onNext(ScanResponse.newBuilder()
                            .setError(okError())
                            .setKey(toByteString(entry.key()))
                            .setValue(toByteString(entry.value()))
                            .setRevision(entry.version())
                            .build());
                        count++;
                    }

                    if (iterator instanceof AutoCloseable closeable) {
                        closeable.close();
                    }
                } catch (Exception e) {
                    logger.error("Error during scan iteration", e);
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
        boolean stale = request.getConsistency() == ReadConsistency.STALE;

        List<CompletableFuture<KeyValue>> futures = new ArrayList<>();

        for (ByteString keyBytes : request.getKeysList()) {
            ByteArray key = toByteArray(keyBytes);

            CompletableFuture<KeyValue> kvFuture;
            if (stale) {
                Optional<ByteArray> value = database.get(key);
                kvFuture = CompletableFuture.completedFuture(buildKeyValue(keyBytes, value));
            } else {
                kvFuture = raftNode.get(key)
                    .thenApply(value -> buildKeyValue(keyBytes, value));
            }
            futures.add(kvFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((__, ex) -> {
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
                    yield raftNode.propose(new Put(
                        toByteArray(put.getKey()),
                        toByteArray(put.getValue()),
                        put.getLeaseId()
                    ));
                }
                case DELETE -> {
                    KvProto.DeleteOp del = writeOp.getDelete();
                    yield raftNode.propose(new Delete(toByteArray(del.getKey())));
                }
                case OP_NOT_SET -> CompletableFuture.failedFuture(
                    new IllegalArgumentException("WriteOp type not set"));
            };
            futures.add(opFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete((__, ex) -> {
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
        long ttlMillis = request.getTtlMillis();

        lessor.grant(ttlMillis)
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
                        .setTtlMillis(ttlMillis)
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
            .whenComplete((__, ex) -> {
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
            .whenComplete((__, ex) -> {
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
        return ByteArray.wrap(bytes.toByteArray());
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
                .setLeaderHint(e.leaderHint().orElse(""))
                .build();
            case TooManyRequestsException e -> Error.newBuilder()
                .setCode(ErrorCode.INTERNAL_ERROR)
                .setMessage("Too many requests, queue size: " + e.queueSize())
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
