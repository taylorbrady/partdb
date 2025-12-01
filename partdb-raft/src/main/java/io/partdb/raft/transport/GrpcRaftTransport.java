package io.partdb.raft.transport;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.RaftTransport;
import io.partdb.raft.rpc.AppendEntriesRequest;
import io.partdb.raft.rpc.AppendEntriesResponse;
import io.partdb.raft.rpc.InstallSnapshotRequest;
import io.partdb.raft.rpc.InstallSnapshotResponse;
import io.partdb.raft.rpc.RequestVoteRequest;
import io.partdb.raft.rpc.RequestVoteResponse;
import io.partdb.raft.rpc.proto.RaftProto;
import io.partdb.raft.rpc.proto.RaftServiceGrpc;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class GrpcRaftTransport implements RaftTransport, AutoCloseable {

    private final RaftTransportConfig config;
    private final ConcurrentHashMap<String, ManagedChannel> channels;

    public GrpcRaftTransport(RaftTransportConfig config) {
        this.config = config;
        this.channels = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String nodeId, RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();

        RaftServiceGrpc.RaftServiceStub stub = getStub(nodeId)
            .withDeadlineAfter(config.requestVoteTimeout().toMillis(), TimeUnit.MILLISECONDS);

        stub.requestVote(ProtobufConverter.toProto(request), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.RequestVoteResponse response) {
                future.complete(ProtobufConverter.fromProto(response));
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
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String nodeId, AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();

        RaftServiceGrpc.RaftServiceStub stub = getStub(nodeId)
            .withDeadlineAfter(config.appendEntriesTimeout().toMillis(), TimeUnit.MILLISECONDS);

        stub.appendEntries(ProtobufConverter.toProto(request), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.AppendEntriesResponse response) {
                future.complete(ProtobufConverter.fromProto(response));
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
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(String nodeId, InstallSnapshotRequest request) {
        CompletableFuture<InstallSnapshotResponse> future = new CompletableFuture<>();

        RaftServiceGrpc.RaftServiceStub stub = getStub(nodeId)
            .withDeadlineAfter(config.installSnapshotTimeout().toMillis(), TimeUnit.MILLISECONDS);

        StreamObserver<RaftProto.InstallSnapshotResponse> responseObserver =
            new StreamObserver<>() {
                @Override
                public void onNext(RaftProto.InstallSnapshotResponse response) {
                    future.complete(ProtobufConverter.fromProto(response));
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                }
            };

        StreamObserver<RaftProto.SnapshotChunk> requestObserver = stub.installSnapshot(responseObserver);

        try {
            String snapshotId = UUID.randomUUID().toString();
            byte[] data = request.data();
            int offset = 0;
            int chunkSize = config.snapshotChunkSize();

            while (offset < data.length) {
                int length = Math.min(chunkSize, data.length - offset);
                byte[] chunkData = new byte[length];
                System.arraycopy(data, offset, chunkData, 0, length);

                RaftProto.SnapshotChunk chunk = RaftProto.SnapshotChunk.newBuilder()
                    .setSnapshotId(snapshotId)
                    .setTerm(request.term())
                    .setLeaderId(request.leaderId())
                    .setLastIncludedIndex(request.lastIncludedIndex())
                    .setLastIncludedTerm(request.lastIncludedTerm())
                    .setOffset(offset)
                    .setData(ByteString.copyFrom(chunkData))
                    .setDone(offset + length >= data.length)
                    .build();

                requestObserver.onNext(chunk);
                offset += length;
            }

            requestObserver.onCompleted();
        } catch (Exception e) {
            requestObserver.onError(e);
            future.completeExceptionally(e);
        }

        return future;
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

    private RaftServiceGrpc.RaftServiceStub getStub(String nodeId) {
        ManagedChannel channel = channels.computeIfAbsent(nodeId, id -> {
            String address = config.peerAddressMap().get(id);
            if (address == null) {
                throw new IllegalArgumentException("Unknown peer: " + id);
            }

            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);

            if (config.tlsEnabled()) {
                builder.useTransportSecurity();
            } else {
                builder.usePlaintext();
            }

            builder.keepAliveTime(30, TimeUnit.SECONDS);
            builder.keepAliveTimeout(10, TimeUnit.SECONDS);

            return builder.build();
        });

        return RaftServiceGrpc.newStub(channel);
    }
}
