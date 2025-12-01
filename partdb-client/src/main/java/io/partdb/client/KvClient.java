package io.partdb.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.protocol.kv.proto.KvProto;
import io.partdb.protocol.kv.proto.KvServiceGrpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class KvClient implements AutoCloseable {

    private final KvClientConfig config;
    private final ConcurrentHashMap<String, ManagedChannel> channels;
    private final AtomicReference<String> currentLeader;
    private final AtomicInteger roundRobinIndex;
    private final ScheduledExecutorService scheduler;

    public KvClient(KvClientConfig config) {
        this.config = config;
        this.channels = new ConcurrentHashMap<>();
        this.currentLeader = new AtomicReference<>(null);
        this.roundRobinIndex = new AtomicInteger(0);
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "kv-client-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public CompletableFuture<Optional<ByteArray>> get(ByteArray key) {
        return get(key, ReadConsistency.LINEARIZABLE);
    }

    public CompletableFuture<Optional<ByteArray>> get(ByteArray key, ReadConsistency consistency) {
        KvProto.GetRequest request = KvProto.GetRequest.newBuilder()
            .setHeader(buildHeader())
            .setKey(ByteString.copyFrom(key.toByteArray()))
            .setConsistency(toProtoConsistency(consistency))
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Optional<ByteArray>> future = new CompletableFuture<>();
            getStub().get(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.GetResponse response) {
                    if (response.hasError() && response.getError().getCode() == KvProto.ErrorCode.NOT_FOUND) {
                        future.complete(Optional.empty());
                    } else if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(Optional.of(ByteArray.copyOf(response.getValue().toByteArray())));
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    public CompletableFuture<Void> put(ByteArray key, ByteArray value) {
        return put(key, value, 0);
    }

    public CompletableFuture<Void> put(ByteArray key, ByteArray value, long leaseId) {
        KvProto.PutRequest request = KvProto.PutRequest.newBuilder()
            .setHeader(buildHeader())
            .setKey(ByteString.copyFrom(key.toByteArray()))
            .setValue(ByteString.copyFrom(value.toByteArray()))
            .setLeaseId(leaseId)
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            getStub().put(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.PutResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    public CompletableFuture<Void> delete(ByteArray key) {
        KvProto.DeleteRequest request = KvProto.DeleteRequest.newBuilder()
            .setHeader(buildHeader())
            .setKey(ByteString.copyFrom(key.toByteArray()))
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            getStub().delete(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.DeleteResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    public CompletableFuture<CloseableIterator<KeyValue>> scan(ByteArray startKey, ByteArray endKey) {
        return scan(startKey, endKey, 0, ReadConsistency.LINEARIZABLE);
    }

    public CompletableFuture<CloseableIterator<KeyValue>> scan(
        ByteArray startKey,
        ByteArray endKey,
        int limit,
        ReadConsistency consistency
    ) {
        KvProto.ScanRequest request = KvProto.ScanRequest.newBuilder()
            .setHeader(buildHeader())
            .setStartKey(ByteString.copyFrom(startKey.toByteArray()))
            .setEndKey(ByteString.copyFrom(endKey.toByteArray()))
            .setLimit(limit)
            .setConsistency(toProtoConsistency(consistency))
            .build();

        ScanIterator iterator = new ScanIterator();

        getStub().scan(request, new StreamObserver<>() {
            @Override
            public void onNext(KvProto.ScanResponse response) {
                if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                    iterator.completeWithError(new KvClientException.ClusterUnavailable(
                        response.getError().getMessage()));
                } else {
                    iterator.addResult(new KeyValue(
                        ByteArray.copyOf(response.getKey().toByteArray()),
                        ByteArray.copyOf(response.getValue().toByteArray()),
                        response.getRevision()
                    ));
                }
            }

            @Override
            public void onError(Throwable t) {
                iterator.completeWithError(t);
            }

            @Override
            public void onCompleted() {
                iterator.complete();
            }
        });

        return CompletableFuture.completedFuture(iterator);
    }

    public CompletableFuture<List<KeyValue>> batchGet(List<ByteArray> keys) {
        return batchGet(keys, ReadConsistency.LINEARIZABLE);
    }

    public CompletableFuture<List<KeyValue>> batchGet(List<ByteArray> keys, ReadConsistency consistency) {
        KvProto.BatchGetRequest.Builder builder = KvProto.BatchGetRequest.newBuilder()
            .setHeader(buildHeader())
            .setConsistency(toProtoConsistency(consistency));

        for (ByteArray key : keys) {
            builder.addKeys(ByteString.copyFrom(key.toByteArray()));
        }

        KvProto.BatchGetRequest request = builder.build();

        return executeWithRetry(() -> {
            CompletableFuture<List<KeyValue>> future = new CompletableFuture<>();
            getStub().batchGet(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.BatchGetResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        List<KeyValue> results = new ArrayList<>();
                        for (KvProto.KeyValue kv : response.getValuesList()) {
                            if (kv.getFound()) {
                                results.add(new KeyValue(
                                    ByteArray.copyOf(kv.getKey().toByteArray()),
                                    ByteArray.copyOf(kv.getValue().toByteArray()),
                                    kv.getRevision()
                                ));
                            }
                        }
                        future.complete(results);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    public CompletableFuture<Void> batchWrite(List<WriteOp> ops) {
        KvProto.BatchWriteRequest.Builder builder = KvProto.BatchWriteRequest.newBuilder()
            .setHeader(buildHeader());

        for (WriteOp op : ops) {
            KvProto.WriteOp.Builder opBuilder = KvProto.WriteOp.newBuilder();
            switch (op) {
                case WriteOp.Put put -> opBuilder.setPut(KvProto.PutOp.newBuilder()
                    .setKey(ByteString.copyFrom(put.key().toByteArray()))
                    .setValue(ByteString.copyFrom(put.value().toByteArray()))
                    .setLeaseId(put.leaseId())
                    .build());
                case WriteOp.Delete delete -> opBuilder.setDelete(KvProto.DeleteOp.newBuilder()
                    .setKey(ByteString.copyFrom(delete.key().toByteArray()))
                    .build());
            }
            builder.addOps(opBuilder.build());
        }

        KvProto.BatchWriteRequest request = builder.build();

        return executeWithRetry(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            getStub().batchWrite(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.BatchWriteResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    public CompletableFuture<Lease> grantLease(Duration ttl) {
        KvProto.GrantLeaseRequest request = KvProto.GrantLeaseRequest.newBuilder()
            .setHeader(buildHeader())
            .setTtlMillis(ttl.toMillis())
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Lease> future = new CompletableFuture<>();
            getStub().grantLease(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.GrantLeaseResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        Lease lease = new Lease(
                            response.getLeaseId(),
                            Duration.ofMillis(response.getTtlMillis()),
                            scheduler,
                            KvClient.this::keepAliveLease,
                            KvClient.this::revokeLease
                        );
                        future.complete(lease);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    private CompletableFuture<Void> keepAliveLease(long leaseId) {
        KvProto.KeepAliveLeaseRequest request = KvProto.KeepAliveLeaseRequest.newBuilder()
            .setHeader(buildHeader())
            .setLeaseId(leaseId)
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            getStub().keepAliveLease(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.KeepAliveLeaseResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    private CompletableFuture<Void> revokeLease(long leaseId) {
        KvProto.RevokeLeaseRequest request = KvProto.RevokeLeaseRequest.newBuilder()
            .setHeader(buildHeader())
            .setLeaseId(leaseId)
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            getStub().revokeLease(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.RevokeLeaseResponse response) {
                    if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            });
            return future;
        });
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }

    private <T> CompletableFuture<T> executeWithRetry(java.util.function.Supplier<CompletableFuture<T>> operation) {
        return executeWithRetry(operation, 0);
    }

    private <T> CompletableFuture<T> executeWithRetry(
        java.util.function.Supplier<CompletableFuture<T>> operation,
        int attempt
    ) {
        return operation.get()
            .exceptionallyCompose(ex -> {
                if (attempt >= config.maxRetries()) {
                    return CompletableFuture.failedFuture(
                        new KvClientException.ClusterUnavailable("All retries exhausted", ex));
                }

                if (ex instanceof NotLeaderException nle) {
                    if (nle.leaderHint != null && !nle.leaderHint.isEmpty()) {
                        currentLeader.set(nle.leaderHint);
                    } else {
                        currentLeader.set(null);
                    }
                    return executeWithRetry(operation, attempt + 1);
                }

                if (isRetryable(ex)) {
                    currentLeader.set(null);
                    return CompletableFuture.supplyAsync(() -> null,
                            CompletableFuture.delayedExecutor(config.retryDelay().toMillis(), TimeUnit.MILLISECONDS))
                        .thenCompose(v -> executeWithRetry(operation, attempt + 1));
                }

                return CompletableFuture.failedFuture(ex);
            })
            .orTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean isRetryable(Throwable ex) {
        if (ex instanceof java.util.concurrent.TimeoutException) {
            return true;
        }
        if (ex instanceof io.grpc.StatusRuntimeException sre) {
            return switch (sre.getStatus().getCode()) {
                case UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED -> true;
                default -> false;
            };
        }
        return ex.getCause() != null && isRetryable(ex.getCause());
    }

    private <T> void handleError(KvProto.Error error, CompletableFuture<T> future) {
        switch (error.getCode()) {
            case NOT_FOUND -> future.complete(null);
            case NOT_LEADER -> future.completeExceptionally(
                new NotLeaderException(error.getLeaderHint()));
            case CONFLICT -> future.completeExceptionally(
                new KvClientException.Conflict(error.getMessage()));
            default -> future.completeExceptionally(
                new KvClientException.ClusterUnavailable(error.getMessage()));
        }
    }

    private KvProto.RequestHeader buildHeader() {
        return KvProto.RequestHeader.newBuilder()
            .setRequestId(UUID.randomUUID().toString())
            .setTimeoutMs(config.requestTimeout().toMillis())
            .build();
    }

    private KvProto.ReadConsistency toProtoConsistency(ReadConsistency consistency) {
        return switch (consistency) {
            case LINEARIZABLE -> KvProto.ReadConsistency.LINEARIZABLE;
            case STALE -> KvProto.ReadConsistency.STALE;
        };
    }

    private KvServiceGrpc.KvServiceStub getStub() {
        String endpoint = selectEndpoint();
        ManagedChannel channel = channels.computeIfAbsent(endpoint, this::createChannel);
        return KvServiceGrpc.newStub(channel)
            .withDeadlineAfter(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private String selectEndpoint() {
        String leader = currentLeader.get();
        if (leader != null) {
            return leader;
        }
        List<String> endpoints = config.endpoints();
        int idx = roundRobinIndex.getAndIncrement() % endpoints.size();
        return endpoints.get(idx);
    }

    private ManagedChannel createChannel(String endpoint) {
        String[] parts = endpoint.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        return ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveTimeout(10, TimeUnit.SECONDS)
            .build();
    }

    private static final class NotLeaderException extends RuntimeException {
        final String leaderHint;

        NotLeaderException(String leaderHint) {
            this.leaderHint = leaderHint;
        }
    }
}
