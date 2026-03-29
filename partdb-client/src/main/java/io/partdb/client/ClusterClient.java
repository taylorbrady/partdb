package io.partdb.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.protocol.cluster.proto.ClusterProto.ErrorCode;
import io.partdb.protocol.cluster.proto.ClusterProto.MemberListRequest;
import io.partdb.protocol.cluster.proto.ClusterProto.MemberListResponse;
import io.partdb.protocol.cluster.proto.ClusterProto.RequestHeader;
import io.partdb.protocol.cluster.proto.ClusterProto.StatusRequest;
import io.partdb.protocol.cluster.proto.ClusterProto.StatusResponse;
import io.partdb.protocol.cluster.proto.ClusterServiceGrpc;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class ClusterClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final ClusterServiceGrpc.ClusterServiceStub stub;
    private final long timeoutMs;

    public ClusterClient(String endpoint, long timeoutMs) {
        String[] parts = endpoint.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();
        this.stub = ClusterServiceGrpc.newStub(channel);
        this.timeoutMs = timeoutMs;
    }

    public CompletableFuture<StatusResponse> status() {
        var request = StatusRequest.newBuilder()
            .setHeader(buildHeader())
            .build();

        var future = new CompletableFuture<StatusResponse>();
        stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
            .status(request, new StreamObserver<>() {
                @Override
                public void onNext(StatusResponse response) {
                    if (response.hasError() && response.getError().getCode() != ErrorCode.OK) {
                        future.completeExceptionally(
                            new ClusterClientException(response.getError().getMessage()));
                    } else {
                        future.complete(response);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {}
            });
        return future;
    }

    public CompletableFuture<MemberListResponse> memberList() {
        var request = MemberListRequest.newBuilder()
            .setHeader(buildHeader())
            .build();

        var future = new CompletableFuture<MemberListResponse>();
        stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
            .memberList(request, new StreamObserver<>() {
                @Override
                public void onNext(MemberListResponse response) {
                    if (response.hasError() && response.getError().getCode() != ErrorCode.OK) {
                        future.completeExceptionally(
                            new ClusterClientException(response.getError().getMessage()));
                    } else {
                        future.complete(response);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {}
            });
        return future;
    }

    private RequestHeader buildHeader() {
        return RequestHeader.newBuilder()
            .setRequestId(UUID.randomUUID().toString())
            .setTimeoutMs(timeoutMs)
            .build();
    }

    @Override
    public void close() {
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
}
