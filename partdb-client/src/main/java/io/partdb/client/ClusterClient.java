package io.partdb.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.grpc.cluster.proto.ClusterProto.ErrorCode;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberListRequest;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberListResponse;
import io.partdb.grpc.cluster.proto.ClusterProto.RequestHeader;
import io.partdb.grpc.cluster.proto.ClusterProto.StatusRequest;
import io.partdb.grpc.cluster.proto.ClusterProto.StatusResponse;
import io.partdb.grpc.cluster.proto.ClusterServiceGrpc;

import java.util.List;
import java.util.Optional;
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

    public CompletableFuture<ClusterStatus> status() {
        var request = StatusRequest.newBuilder()
            .setHeader(buildHeader())
            .build();

        var future = new CompletableFuture<ClusterStatus>();
        stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
            .status(request, new StreamObserver<>() {
                @Override
                public void onNext(StatusResponse response) {
                    if (response.hasError() && response.getError().getCode() != ErrorCode.OK) {
                        future.completeExceptionally(
                            new ClusterClientException(response.getError().getMessage()));
                    } else {
                        future.complete(toClusterStatus(response));
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

    public CompletableFuture<ClusterMembership> memberList() {
        var request = MemberListRequest.newBuilder()
            .setHeader(buildHeader())
            .build();

        var future = new CompletableFuture<ClusterMembership>();
        stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
            .memberList(request, new StreamObserver<>() {
                @Override
                public void onNext(MemberListResponse response) {
                    if (response.hasError() && response.getError().getCode() != ErrorCode.OK) {
                        future.completeExceptionally(
                            new ClusterClientException(response.getError().getMessage()));
                    } else {
                        future.complete(toClusterMembership(response));
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

    private static ClusterStatus toClusterStatus(StatusResponse response) {
        return new ClusterStatus(
            response.getNodeId(),
            toClusterNodeRole(response.getRole()),
            response.getTerm(),
            emptyToOptional(response.getLeaderId()),
            response.getCommitIndex(),
            response.getLastAppliedIndex(),
            response.getIsRunning()
        );
    }

    private static ClusterMembership toClusterMembership(MemberListResponse response) {
        List<ClusterMember> members = response.getMembersList().stream()
            .map(member -> new ClusterMember(
                member.getNodeId(),
                emptyToOptional(member.getAddress()),
                toClusterMemberRole(member.getRole()),
                member.getIsLeader(),
                member.getIsSelf()
            ))
            .toList();

        return new ClusterMembership(
            emptyToOptional(response.getLeaderId()),
            members
        );
    }

    private static ClusterNodeRole toClusterNodeRole(io.partdb.grpc.cluster.proto.ClusterProto.NodeRole role) {
        return switch (role) {
            case FOLLOWER -> ClusterNodeRole.FOLLOWER;
            case PRE_CANDIDATE -> ClusterNodeRole.PRE_CANDIDATE;
            case CANDIDATE -> ClusterNodeRole.CANDIDATE;
            case LEADER -> ClusterNodeRole.LEADER;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unknown node role: " + role);
        };
    }

    private static ClusterMemberRole toClusterMemberRole(io.partdb.grpc.cluster.proto.ClusterProto.MemberRole role) {
        return switch (role) {
            case VOTER -> ClusterMemberRole.VOTER;
            case LEARNER -> ClusterMemberRole.LEARNER;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unknown member role: " + role);
        };
    }

    private static Optional<String> emptyToOptional(String value) {
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
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
