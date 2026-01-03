package io.partdb.server.raft;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftTransport;
import io.partdb.server.raft.proto.RaftProto;
import io.partdb.server.raft.proto.RaftServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class GrpcRaftTransport implements RaftTransport {

    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);

    private final GrpcRaftTransportConfig config;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> stubs = new ConcurrentHashMap<>();

    private Server server;

    public GrpcRaftTransport(GrpcRaftTransportConfig config) {
        this.config = config;
    }

    @Override
    public void start(RpcHandler handler) {
        try {
            server = ServerBuilder.forPort(config.port())
                .addService(new RaftServiceImpl(handler))
                .intercept(RaftServiceImpl.senderIdInterceptor())
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
    public CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request) {
        var stub = getOrCreateStub(to);
        if (stub == null) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown peer: " + to)
            );
        }

        return switch (request) {
            case RaftMessage.RequestVote msg -> sendRequestVote(stub, msg);
            case RaftMessage.PreVote msg -> sendPreVote(stub, msg);
            case RaftMessage.AppendEntries msg -> sendAppendEntries(stub, msg);
            case RaftMessage.InstallSnapshot msg -> sendInstallSnapshot(stub, msg);
            case RaftMessage.ReadIndex msg -> sendReadIndex(stub, msg);
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
            String address = config.peerAddresses().get(id);
            if (address == null) {
                return null;
            }
            ManagedChannel channel = getOrCreateChannel(id, address);
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
                        headers.put(RaftServiceImpl.SENDER_ID_KEY, config.localNodeId());
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private ManagedChannel getOrCreateChannel(String peerId, String address) {
        return channels.computeIfAbsent(peerId, _ -> {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        });
    }

    private CompletableFuture<RaftMessage.Response> sendRequestVote(
            RaftServiceGrpc.RaftServiceStub stub,
            RaftMessage.RequestVote msg) {
        var future = new CompletableFuture<RaftMessage.Response>();
        stub.requestVote(ProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.RequestVoteResponse response) {
                future.complete(ProtoConverters.fromProto(response));
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

    private CompletableFuture<RaftMessage.Response> sendPreVote(
            RaftServiceGrpc.RaftServiceStub stub,
            RaftMessage.PreVote msg) {
        var future = new CompletableFuture<RaftMessage.Response>();
        stub.preVote(ProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.PreVoteResponse response) {
                future.complete(ProtoConverters.fromProto(response));
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

    private CompletableFuture<RaftMessage.Response> sendAppendEntries(
            RaftServiceGrpc.RaftServiceStub stub,
            RaftMessage.AppendEntries msg) {
        var future = new CompletableFuture<RaftMessage.Response>();
        stub.appendEntries(ProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.AppendEntriesResponse response) {
                future.complete(ProtoConverters.fromProto(response));
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

    private CompletableFuture<RaftMessage.Response> sendInstallSnapshot(
            RaftServiceGrpc.RaftServiceStub stub,
            RaftMessage.InstallSnapshot msg) {
        var future = new CompletableFuture<RaftMessage.Response>();

        var responseObserver = new StreamObserver<RaftProto.InstallSnapshotResponse>() {
            @Override
            public void onNext(RaftProto.InstallSnapshotResponse response) {
                future.complete(ProtoConverters.fromProto(response));
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
            .setHeader(ProtoConverters.toSnapshotHeader(msg))
            .build());

        byte[] data = msg.data();
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

    private CompletableFuture<RaftMessage.Response> sendReadIndex(
            RaftServiceGrpc.RaftServiceStub stub,
            RaftMessage.ReadIndex msg) {
        var future = new CompletableFuture<RaftMessage.Response>();
        stub.readIndex(ProtoConverters.toProto(msg), new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.ReadIndexResponse response) {
                future.complete(ProtoConverters.fromProto(response));
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
