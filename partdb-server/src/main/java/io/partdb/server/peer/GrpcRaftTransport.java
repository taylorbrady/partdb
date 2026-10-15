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
import io.partdb.raft.Envelope;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class GrpcRaftTransport implements RaftTransport {

    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);
    private static final int SNAPSHOT_CHUNK_BYTES = 1 << 20;
    private static final int STREAM_CAPACITY = 4096;

    private final String nodeId;
    private final int port;
    private final Map<String, PeerEndpoint> peers;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, PeerStream> streams = new ConcurrentHashMap<>();
    private Receiver receiver;
    private Server server;

    public GrpcRaftTransport(String nodeId, int port, Map<String, PeerEndpoint> peers) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.port = port;
        this.peers = Map.copyOf(Objects.requireNonNull(peers, "peers must not be null"));
    }

    @Override
    public void start(Receiver receiver) {
        this.receiver = receiver;
        try {
            server = NettyServerBuilder.forPort(port)
                .addService(new RaftService(nodeId, receiver))
                .intercept(RaftService.senderInterceptor())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build()
                .start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start raft transport on port " + port, e);
        }
        log.atInfo().addKeyValue("nodeId", nodeId).addKeyValue("port", port).log("Raft transport started");
    }

    @Override
    public void send(Envelope envelope) {
        var stream = streams.computeIfAbsent(envelope.to(), this::openStream);
        if (stream == null) {
            receiver.unreachable(envelope.to());
            return;
        }
        stream.send(envelope.message());
    }

    @Override
    public CompletionStage<Void> sendSnapshot(String to, RaftMessage.InstallSnapshot request, Path payload) {
        var stub = stub(to);
        if (stub == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("unknown peer " + to));
        }
        FileChannel file;
        try {
            file = FileChannel.open(payload, StandardOpenOption.READ);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        var upload = new SnapshotUpload(RaftProtoMapper.toProto(request), file);
        stub.installSnapshot(upload);
        return upload.result;
    }

    @Override
    public void close() {
        streams.values().forEach(PeerStream::close);
        streams.clear();
        if (server != null) {
            server.shutdownNow();
            awaitTermination(server::awaitTermination);
        }
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
            awaitTermination(channel::awaitTermination);
        }
        channels.clear();
        log.atInfo().addKeyValue("nodeId", nodeId).log("Raft transport stopped");
    }

    private PeerStream openStream(String peer) {
        var stub = stub(peer);
        return stub == null ? null : new PeerStream(peer, stub, receiver, STREAM_CAPACITY);
    }

    private RaftServiceGrpc.RaftServiceStub stub(String peer) {
        PeerEndpoint endpoint = peers.get(peer);
        if (endpoint == null) {
            return null;
        }
        var channel = channels.computeIfAbsent(peer, _ -> NettyChannelBuilder
            .forAddress(endpoint.host(), endpoint.port())
            .usePlaintext()
            .build());
        return RaftServiceGrpc.newStub(channel).withInterceptors(new SenderInterceptor());
    }

    private static void awaitTermination(Termination termination) {
        try {
            termination.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
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
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private ClientCallStreamObserver<RaftProto.InstallSnapshotRequest> requests;
        private boolean headerSent;
        private boolean finished;

        private SnapshotUpload(RaftProto.SnapshotHeader header, FileChannel file) {
            this.header = header;
            this.file = file;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<RaftProto.InstallSnapshotRequest> requests) {
            this.requests = requests;
            requests.setOnReadyHandler(this::pump);
        }

        private synchronized void pump() {
            try {
                while (!finished && requests.isReady()) {
                    if (!headerSent) {
                        requests.onNext(RaftProto.InstallSnapshotRequest.newBuilder().setHeader(header).build());
                        headerSent = true;
                        continue;
                    }
                    ByteBuffer chunk = ByteBuffer.allocate(SNAPSHOT_CHUNK_BYTES);
                    if (file.read(chunk) < 0) {
                        finished = true;
                        closeFile();
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
                result.completeExceptionally(e);
            }
        }

        @Override
        public void onNext(RaftProto.InstallSnapshotResponse value) {
        }

        @Override
        public synchronized void onError(Throwable error) {
            finished = true;
            closeFile();
            result.completeExceptionally(error);
        }

        @Override
        public void onCompleted() {
            closeFile();
            result.complete(null);
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
