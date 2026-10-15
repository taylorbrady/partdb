package io.partdb.server.peer;

import com.google.protobuf.UnsafeByteOperations;
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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.RaftProto;
import io.partdb.server.peer.proto.RaftServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class GrpcRaftTransport implements RaftTransport {

    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);
    private static final int SNAPSHOT_CHUNK_SIZE = 1024 * 1024;

    private final String nodeId;
    private final int port;
    private final Map<String, PeerEndpoint> peers;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceStub> stubs = new ConcurrentHashMap<>();
    private Server server;

    public GrpcRaftTransport(String nodeId, int port, Map<String, PeerEndpoint> peers) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.port = port;
        this.peers = Map.copyOf(Objects.requireNonNull(peers, "peers must not be null"));
    }

    @Override
    public void start(Inbound inbound) {
        try {
            server = NettyServerBuilder.forPort(port)
                .addService(new RaftService(inbound))
                .intercept(RaftService.senderInterceptor())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build()
                .start();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start raft transport on port " + port, e);
        }
        log.atInfo().addKeyValue("nodeId", nodeId).addKeyValue("port", port).log("Raft transport started");
    }

    @Override
    public CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request) {
        var stub = stub(to);
        if (stub == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown peer: " + to));
        }
        return switch (request) {
            case RaftMessage.RequestVote message ->
                unary(stub::requestVote, RaftProtoMapper.toProto(message), RaftProtoMapper::fromProto);
            case RaftMessage.PreVote message ->
                unary(stub::preVote, RaftProtoMapper.toProto(message), RaftProtoMapper::fromProto);
            case RaftMessage.AppendEntries message ->
                unary(stub::appendEntries, RaftProtoMapper.toProto(message), RaftProtoMapper::fromProto);
            case RaftMessage.ReadIndexRequested message ->
                unary(stub::readIndex, RaftProtoMapper.toProto(message), RaftProtoMapper::fromProto);
            case RaftMessage.InstallSnapshot _ ->
                CompletableFuture.failedFuture(new IllegalArgumentException("Snapshots must be sent with sendSnapshot"));
        };
    }

    @Override
    public CompletableFuture<RaftMessage.InstallSnapshotResponse> sendSnapshot(
        String to,
        RaftMessage.InstallSnapshot request,
        Path payload
    ) {
        var stub = stub(to);
        if (stub == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown peer: " + to));
        }
        FileChannel file;
        try {
            file = FileChannel.open(payload, StandardOpenOption.READ);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        var upload = new SnapshotUpload(RaftProtoMapper.toProto(request), file);
        stub.installSnapshot(upload);
        return upload.response;
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdown();
            awaitTermination(server::awaitTermination, server::shutdownNow);
        }
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            awaitTermination(channel::awaitTermination, channel::shutdownNow);
        }
        channels.clear();
        stubs.clear();
        log.atInfo().addKeyValue("nodeId", nodeId).log("Raft transport stopped");
    }

    private RaftServiceGrpc.RaftServiceStub stub(String peerId) {
        PeerEndpoint endpoint = peers.get(peerId);
        if (endpoint == null) {
            return null;
        }
        return stubs.computeIfAbsent(peerId, id -> {
            ManagedChannel channel = channels.computeIfAbsent(id, _ -> NettyChannelBuilder
                .forAddress(endpoint.host(), endpoint.port())
                .usePlaintext()
                .build());
            return RaftServiceGrpc.newStub(channel).withInterceptors(new SenderInterceptor());
        });
    }

    private static <Req, Resp> CompletableFuture<RaftMessage.Response> unary(
        BiConsumer<Req, StreamObserver<Resp>> call,
        Req request,
        Function<Resp, ? extends RaftMessage.Response> mapper
    ) {
        var future = new CompletableFuture<RaftMessage.Response>();
        call.accept(request, new StreamObserver<>() {
            @Override
            public void onNext(Resp response) {
                future.complete(mapper.apply(response));
            }

            @Override
            public void onError(Throwable error) {
                future.completeExceptionally(error);
            }

            @Override
            public void onCompleted() {
            }
        });
        return future;
    }

    private static void awaitTermination(Termination termination, Runnable forceShutdown) {
        try {
            if (!termination.await(5, TimeUnit.SECONDS)) {
                forceShutdown.run();
                termination.await(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            forceShutdown.run();
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    private interface Termination {
        boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    }

    private final class SenderInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next
        ) {
            return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    headers.put(RaftService.SENDER_KEY, nodeId);
                    super.start(responseListener, headers);
                }
            };
        }
    }

    private static final class SnapshotUpload
        implements ClientResponseObserver<RaftProto.InstallSnapshotRequest, RaftProto.InstallSnapshotResponse> {

        private final RaftProto.SnapshotHeader header;
        private final FileChannel file;
        private final CompletableFuture<RaftMessage.InstallSnapshotResponse> response = new CompletableFuture<>();
        private ClientCallStreamObserver<RaftProto.InstallSnapshotRequest> requests;
        private boolean headerSent;
        private volatile boolean finished;

        private SnapshotUpload(RaftProto.SnapshotHeader header, FileChannel file) {
            this.header = header;
            this.file = file;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<RaftProto.InstallSnapshotRequest> requests) {
            this.requests = requests;
            requests.setOnReadyHandler(this::pump);
        }

        private void pump() {
            try {
                while (!finished && requests.isReady()) {
                    if (!headerSent) {
                        requests.onNext(RaftProto.InstallSnapshotRequest.newBuilder().setHeader(header).build());
                        headerSent = true;
                        continue;
                    }
                    ByteBuffer chunk = ByteBuffer.allocate(SNAPSHOT_CHUNK_SIZE);
                    if (file.read(chunk) < 0) {
                        finished = true;
                        file.close();
                        requests.onCompleted();
                        return;
                    }
                    requests.onNext(RaftProto.InstallSnapshotRequest.newBuilder()
                        .setChunk(UnsafeByteOperations.unsafeWrap(chunk.flip()))
                        .build());
                }
            } catch (IOException e) {
                finished = true;
                closeFile();
                requests.onError(e);
                response.completeExceptionally(e);
            }
        }

        @Override
        public void onNext(RaftProto.InstallSnapshotResponse value) {
            response.complete(RaftProtoMapper.fromProto(value));
        }

        @Override
        public void onError(Throwable error) {
            finished = true;
            closeFile();
            response.completeExceptionally(error);
        }

        @Override
        public void onCompleted() {
            closeFile();
        }

        private void closeFile() {
            try {
                file.close();
            } catch (IOException e) {
                log.atDebug().setCause(e).log("Failed to close snapshot payload");
            }
        }
    }
}
