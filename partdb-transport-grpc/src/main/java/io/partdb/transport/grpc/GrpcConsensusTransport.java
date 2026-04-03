package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.consensus.transport.ConsensusRpc;
import io.partdb.consensus.transport.ConsensusTransport;
import io.partdb.transport.grpc.raft.proto.RaftProto;
import io.partdb.transport.grpc.raft.proto.RaftServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

final class GrpcConsensusTransport implements ConsensusTransport {

    private static final Logger log = LoggerFactory.getLogger(GrpcConsensusTransport.class);

    private final GrpcConsensusTransportConfig config;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> stubs = new ConcurrentHashMap<>();

    private Server server;

    GrpcConsensusTransport(GrpcConsensusTransportConfig config) {
        this.config = config;
    }

    @Override
    public void start(RpcHandler handler) {
        try {
            server = NettyServerBuilder.forPort(config.port())
                .addService(new ConsensusServiceImpl(handler))
                .intercept(ConsensusServiceImpl.senderIdInterceptor())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build()
                .start();
            log.atInfo()
                .addKeyValue("nodeId", config.localNodeId())
                .addKeyValue("port", config.port())
                .log("Raft transport started");
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Raft transport", e);
        }
    }

    @Override
    public CompletableFuture<ConsensusRpc.Response> send(String to, ConsensusRpc.Request request) {
        var stub = getOrCreateStub(to);
        if (stub == null) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown peer: " + to)
            );
        }

        return switch (request) {
            case ConsensusRpc.RequestVote msg -> sendRequestVote(stub, msg);
            case ConsensusRpc.PreVote msg -> sendPreVote(stub, msg);
            case ConsensusRpc.AppendEntries msg -> sendAppendEntries(stub, msg);
            case ConsensusRpc.InstallSnapshot msg -> sendInstallSnapshot(stub, msg);
            case ConsensusRpc.ReadIndex msg -> sendReadIndex(stub, msg);
        };
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                    server.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            }
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
        stubs.clear();
        log.atInfo()
            .addKeyValue("nodeId", config.localNodeId())
            .log("Raft transport stopped");
    }

    private RaftServiceGrpc.RaftServiceStub getOrCreateStub(String peerId) {
        return stubs.computeIfAbsent(peerId, id -> {
            PeerEndpoint endpoint = config.raftPeerEndpoints().get(id);
            if (endpoint == null) {
                return null;
            }
            ManagedChannel channel = getOrCreateChannel(id, endpoint);
            return RaftServiceGrpc.newStub(channel)
                .withInterceptors(senderIdInterceptor());
        });
    }

    private ClientInterceptor senderIdInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(ConsensusServiceImpl.SENDER_ID_KEY, config.localNodeId());
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private ManagedChannel getOrCreateChannel(String peerId, PeerEndpoint endpoint) {
        return channels.computeIfAbsent(peerId, _ -> {
            return NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
                .usePlaintext()
                .build();
        });
    }

    private CompletableFuture<ConsensusRpc.Response> sendRequestVote(
            RaftServiceGrpc.RaftServiceStub stub,
            ConsensusRpc.RequestVote msg) {
        var future = new CompletableFuture<ConsensusRpc.Response>();
        stub.requestVote(ConsensusProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.RequestVoteResponse response) {
                future.complete(ConsensusProtoConverters.fromProto(response));
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

    private CompletableFuture<ConsensusRpc.Response> sendPreVote(
            RaftServiceGrpc.RaftServiceStub stub,
            ConsensusRpc.PreVote msg) {
        var future = new CompletableFuture<ConsensusRpc.Response>();
        stub.preVote(ConsensusProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.PreVoteResponse response) {
                future.complete(ConsensusProtoConverters.fromProto(response));
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

    private CompletableFuture<ConsensusRpc.Response> sendAppendEntries(
            RaftServiceGrpc.RaftServiceStub stub,
            ConsensusRpc.AppendEntries msg) {
        var future = new CompletableFuture<ConsensusRpc.Response>();
        stub.appendEntries(ConsensusProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.AppendEntriesResponse response) {
                future.complete(ConsensusProtoConverters.fromProto(response));
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

    private CompletableFuture<ConsensusRpc.Response> sendInstallSnapshot(
            RaftServiceGrpc.RaftServiceStub stub,
            ConsensusRpc.InstallSnapshot msg) {
        var future = new CompletableFuture<ConsensusRpc.Response>();

        var responseObserver = new StreamObserver<RaftProto.InstallSnapshotResponse>() {
            @Override
            public void onNext(RaftProto.InstallSnapshotResponse response) {
                future.complete(ConsensusProtoConverters.fromProto(response));
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        };

        var requestObserver = stub.installSnapshot(responseObserver);

        requestObserver.onNext(RaftProto.InstallSnapshotRequest.newBuilder()
            .setHeader(ConsensusProtoConverters.toSnapshotHeader(msg))
            .build());

        byte[] data = msg.data().toByteArray();
        int offset = 0;
        int chunkSize = config.snapshotChunkSize();

        while (offset < data.length) {
            int end = Math.min(offset + chunkSize, data.length);
            byte[] chunk = Arrays.copyOfRange(data, offset, end);
            boolean done = (end == data.length);

            requestObserver.onNext(RaftProto.InstallSnapshotRequest.newBuilder()
                .setChunk(RaftProto.SnapshotChunk.newBuilder()
                    .setData(ByteString.copyFrom(chunk))
                    .setDone(done))
                .build());
            offset = end;
        }

        requestObserver.onCompleted();
        return future;
    }

    private CompletableFuture<ConsensusRpc.Response> sendReadIndex(
            RaftServiceGrpc.RaftServiceStub stub,
            ConsensusRpc.ReadIndex msg) {
        var future = new CompletableFuture<ConsensusRpc.Response>();
        stub.readIndex(ConsensusProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.ReadIndexResponse response) {
                future.complete(ConsensusProtoConverters.fromProto(response));
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
}
