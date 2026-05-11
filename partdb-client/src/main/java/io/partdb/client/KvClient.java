package io.partdb.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.kv.proto.KvProto;
import io.partdb.grpc.kv.proto.KvServiceGrpc;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class KvClient implements AutoCloseable {

    private final KvClientConfig config;
    private final ConcurrentHashMap<ServerEndpoint, ManagedChannel> channels;
    private final AtomicReference<ServerEndpoint> currentLeader;
    private final AtomicInteger roundRobinIndex;

    public KvClient(KvClientConfig config) {
        this.config = config;
        this.channels = new ConcurrentHashMap<>();
        this.currentLeader = new AtomicReference<>(null);
        this.roundRobinIndex = new AtomicInteger(0);
    }

    public CompletableFuture<Optional<Bytes>> get(Bytes key) {
        return get(key, ReadConsistency.LINEARIZABLE);
    }

    public CompletableFuture<Optional<Bytes>> get(Bytes key, ReadConsistency consistency) {
        KvProto.GetRequest request = KvProto.GetRequest.newBuilder()
            .setHeader(buildHeader())
            .setKey(toByteString(key))
            .setConsistency(toProtoConsistency(consistency))
            .build();

        return executeWithRetry(() -> {
            CompletableFuture<Optional<Bytes>> future = new CompletableFuture<>();
            getStub().get(request, new StreamObserver<>() {
                @Override
                public void onNext(KvProto.GetResponse response) {
                    if (response.hasError() && response.getError().getCode() == KvProto.ErrorCode.NOT_FOUND) {
                        future.complete(Optional.empty());
                    } else if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                        handleError(response.getError(), future);
                    } else {
                        future.complete(Optional.of(Bytes.copyOf(response.getValue().toByteArray())));
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

    public CompletableFuture<Void> put(Bytes key, Bytes value) {
        KvProto.PutRequest request = KvProto.PutRequest.newBuilder()
            .setHeader(buildHeader())
            .setKey(toByteString(key))
            .setValue(toByteString(value))
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

    public CompletableFuture<Void> delete(Bytes key) {
        KvProto.DeleteRequest request = KvProto.DeleteRequest.newBuilder()
            .setHeader(buildHeader())
            .setKey(toByteString(key))
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

    public CompletableFuture<ScanCursor> scan(Optional<Bytes> startKey, Optional<Bytes> endKey) {
        return scan(startKey, endKey, 0, ReadConsistency.LINEARIZABLE);
    }

    public CompletableFuture<ScanCursor> scan(
        Optional<Bytes> startKey,
        Optional<Bytes> endKey,
        int limit,
        ReadConsistency consistency
    ) {
        KvProto.ScanRequest request = KvProto.ScanRequest.newBuilder()
            .setHeader(buildHeader())
            .setStartKey(startKey.map(KvClient::toByteString).orElse(ByteString.EMPTY))
            .setEndKey(endKey.map(KvClient::toByteString).orElse(ByteString.EMPTY))
            .setLimit(limit)
            .setConsistency(toProtoConsistency(consistency))
            .build();

        return startScan(request, 0);
    }

    public CompletableFuture<Void> batchWrite(List<WriteOp> ops) {
        KvProto.BatchWriteRequest.Builder builder = KvProto.BatchWriteRequest.newBuilder()
            .setHeader(buildHeader());

        for (WriteOp op : ops) {
            KvProto.WriteOp.Builder opBuilder = KvProto.WriteOp.newBuilder();
            switch (op) {
                case WriteOp.Put put -> opBuilder.setPut(KvProto.PutOp.newBuilder()
                    .setKey(toByteString(put.key()))
                    .setValue(toByteString(put.value()))
                    .build());
                case WriteOp.Delete delete -> opBuilder.setDelete(KvProto.DeleteOp.newBuilder()
                    .setKey(toByteString(delete.key()))
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

    @Override
    public void close() {
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
                    if (nle.leaderHint != null) {
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
            default -> future.completeExceptionally(toException(error));
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

    private static ByteString toByteString(Bytes bytes) {
        return ByteString.copyFrom(bytes.asReadOnlyByteBuffer());
    }

    private KvServiceGrpc.KvServiceStub getStub() {
        ServerEndpoint endpoint = selectEndpoint();
        return stubFor(endpoint);
    }

    private KvServiceGrpc.KvServiceStub stubFor(ServerEndpoint endpoint) {
        ManagedChannel channel = channels.computeIfAbsent(endpoint, this::createChannel);
        return KvServiceGrpc.newStub(channel)
            .withDeadlineAfter(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private ServerEndpoint selectEndpoint() {
        ServerEndpoint leader = currentLeader.get();
        if (leader != null) {
            return leader;
        }
        List<ServerEndpoint> endpoints = config.endpoints();
        int idx = roundRobinIndex.getAndIncrement() % endpoints.size();
        return endpoints.get(idx);
    }

    private ManagedChannel createChannel(ServerEndpoint endpoint) {
        return GrpcClientChannels.createChannel(endpoint, config.connectTimeout());
    }

    private CompletableFuture<ScanCursor> startScan(KvProto.ScanRequest request, int attempt) {
        ServerEndpoint endpoint = selectEndpoint();
        var future = new CompletableFuture<ScanCursor>();
        var cursor = new GrpcScanCursor();
        var active = new AtomicBoolean(true);
        var published = new AtomicBoolean(false);

        stubFor(endpoint).scan(request, new StreamObserver<>() {
            @Override
            public void onNext(KvProto.ScanResponse response) {
                if (!active.get()) {
                    return;
                }

                if (response.hasError() && response.getError().getCode() != KvProto.ErrorCode.OK) {
                    RuntimeException exception = toException(response.getError());
                    if (!published.get() && shouldRetryScan(exception, attempt)) {
                        active.set(false);
                        cursor.close();
                        retryScan(request, attempt, exception, future);
                        return;
                    }

                    failScan(cursor, future, active, exception);
                    return;
                }

                cursor.addResult(new KeyValue(
                    Bytes.copyOf(response.getKey().toByteArray()),
                    Bytes.copyOf(response.getValue().toByteArray()),
                    response.getRevision()
                ));
                published.set(true);
                future.complete(cursor);
            }

            @Override
            public void onError(Throwable t) {
                if (!active.get()) {
                    return;
                }

                if (!published.get() && isRetryable(t) && attempt < config.maxRetries()) {
                    active.set(false);
                    currentLeader.set(null);
                    cursor.close();
                    retryScan(request, attempt, t, future);
                    return;
                }

                failScan(cursor, future, active, t);
            }

            @Override
            public void onCompleted() {
                if (!active.getAndSet(false)) {
                    return;
                }
                cursor.complete();
                future.complete(cursor);
            }
        });

        return future;
    }

    private void retryScan(
        KvProto.ScanRequest request,
        int attempt,
        Throwable cause,
        CompletableFuture<ScanCursor> future
    ) {
        if (attempt >= config.maxRetries()) {
            future.completeExceptionally(new KvClientException.ClusterUnavailable("All retries exhausted", cause));
            return;
        }

        CompletableFuture<ScanCursor> retry = cause instanceof NotLeaderException
            ? startScan(request, attempt + 1)
            : CompletableFuture
                .runAsync(
                    () -> { },
                    CompletableFuture.delayedExecutor(config.retryDelay().toMillis(), TimeUnit.MILLISECONDS)
                )
                .thenCompose(ignored -> startScan(request, attempt + 1));

        retry.whenComplete((cursor, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(cursor);
            }
        });
    }

    private boolean shouldRetryScan(RuntimeException exception, int attempt) {
        if (attempt >= config.maxRetries()) {
            return false;
        }
        if (exception instanceof NotLeaderException notLeader) {
            currentLeader.set(notLeader.leaderHint);
            return true;
        }
        return false;
    }

    private RuntimeException toException(KvProto.Error error) {
        return switch (error.getCode()) {
            case NOT_LEADER -> new NotLeaderException(ServerEndpoint.tryParse(error.getLeaderHint()).orElse(null));
            default -> new KvClientException.ClusterUnavailable(error.getMessage());
        };
    }

    private static void failScan(
        GrpcScanCursor cursor,
        CompletableFuture<ScanCursor> future,
        AtomicBoolean active,
        Throwable failure
    ) {
        if (!active.getAndSet(false)) {
            return;
        }
        if (!future.completeExceptionally(failure)) {
            cursor.completeWithError(failure);
        }
    }

    private static final class NotLeaderException extends RuntimeException {
        final ServerEndpoint leaderHint;

        NotLeaderException(ServerEndpoint leaderHint) {
            this.leaderHint = leaderHint;
        }
    }
}
