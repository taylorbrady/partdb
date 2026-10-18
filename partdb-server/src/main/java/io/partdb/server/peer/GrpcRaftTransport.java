package io.partdb.server.peer;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.partdb.raft.Envelope;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.ClusterId;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.PeerProto.InstallSnapshotRequest;
import io.partdb.server.peer.proto.PeerProto.InstallSnapshotResponse;
import io.partdb.server.peer.proto.PeerProto.SnapshotHeader;
import io.partdb.server.peer.proto.RaftPeerGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class GrpcRaftTransport implements RaftTransport, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);
    private static final int SNAPSHOT_CHUNK_BYTES = 1 << 20;
    private static final long STREAM_CAPACITY_BYTES = 64L << 20;
    private static final int MAX_MESSAGE_BYTES = 64 << 20;
    private static final Duration KEEPALIVE = Duration.ofSeconds(10);
    private static final Duration KEEPALIVE_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SNAPSHOT_BASE_DEADLINE = Duration.ofSeconds(30);
    private static final long SNAPSHOT_MIN_BYTES_PER_SECOND = 4L << 20;

    private final ClusterId clusterId;
    private final NodeId nodeId;
    private final int port;
    private final Map<NodeId, PeerEndpoint> peers;
    private final Map<NodeId, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<NodeId, PeerStream> streams = new ConcurrentHashMap<>();
    private volatile Receiver receiver;
    private volatile Server server;

    public GrpcRaftTransport(ClusterId clusterId, NodeId nodeId, int port, Map<NodeId, PeerEndpoint> peers) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId must not be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.port = port;
        this.peers = Map.copyOf(Objects.requireNonNull(peers, "peers must not be null"));
    }

    @Override
    public void start(Receiver receiver) {
        this.receiver = Objects.requireNonNull(receiver, "receiver must not be null");
        try {
            server = NettyServerBuilder.forPort(port)
                .addService(new RaftService(nodeId, receiver))
                .intercept(PeerHeaders.incoming(clusterId, nodeId))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .permitKeepAliveTime(KEEPALIVE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .permitKeepAliveWithoutCalls(true)
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
    public CompletionStage<Void> sendSnapshot(NodeId to, long term, SnapshotSource snapshot) {
        var stub = stub(to);
        if (stub == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("unknown peer " + to));
        }
        long seconds = SNAPSHOT_BASE_DEADLINE.toSeconds() + snapshot.info().size() / SNAPSHOT_MIN_BYTES_PER_SECOND;
        var upload = new SnapshotUpload(RaftProtoMapper.toProto(term, snapshot.info()), snapshot.channel());
        stub.withDeadlineAfter(seconds, TimeUnit.SECONDS).installSnapshot(upload);
        return upload.result;
    }

    @Override
    public void close() {
        streams.values().forEach(PeerStream::close);
        streams.clear();
        var running = server;
        if (running != null) {
            running.shutdownNow();
            awaitTermination(running::awaitTermination);
        }
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
            awaitTermination(channel::awaitTermination);
        }
        channels.clear();
        log.atInfo().addKeyValue("nodeId", nodeId).log("Raft transport stopped");
    }

    private PeerStream openStream(NodeId peer) {
        var stub = stub(peer);
        return stub == null ? null : new PeerStream(peer, stub, receiver, STREAM_CAPACITY_BYTES);
    }

    private RaftPeerGrpc.RaftPeerStub stub(NodeId peer) {
        PeerEndpoint endpoint = peers.get(peer);
        if (endpoint == null) {
            return null;
        }
        var channel = channels.computeIfAbsent(peer, _ -> NettyChannelBuilder
            .forAddress(endpoint.host(), endpoint.port())
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES)
            .keepAliveTime(KEEPALIVE.toMillis(), TimeUnit.MILLISECONDS)
            .keepAliveTimeout(KEEPALIVE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .keepAliveWithoutCalls(true)
            .build());
        return RaftPeerGrpc.newStub(channel)
            .withInterceptors(PeerHeaders.outgoing(clusterId, nodeId, peer))
            .withMaxOutboundMessageSize(MAX_MESSAGE_BYTES);
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

    private static final class SnapshotUpload implements ClientResponseObserver<InstallSnapshotRequest, InstallSnapshotResponse> {

        private final SnapshotHeader header;
        private final FileChannel file;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private ClientCallStreamObserver<InstallSnapshotRequest> requests;
        private long position;
        private boolean headerSent;
        private boolean finished;

        private SnapshotUpload(SnapshotHeader header, FileChannel file) {
            this.header = header;
            this.file = file;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<InstallSnapshotRequest> requests) {
            this.requests = requests;
            requests.setOnReadyHandler(this::pump);
        }

        private synchronized void pump() {
            try {
                while (!finished && requests.isReady()) {
                    if (!headerSent) {
                        requests.onNext(InstallSnapshotRequest.newBuilder().setHeader(header).build());
                        headerSent = true;
                        continue;
                    }
                    ByteBuffer chunk = ByteBuffer.allocate(SNAPSHOT_CHUNK_BYTES);
                    int read = file.read(chunk, position);
                    if (read < 0) {
                        finished = true;
                        requests.onCompleted();
                        return;
                    }
                    position += read;
                    requests.onNext(InstallSnapshotRequest.newBuilder()
                        .setChunk(UnsafeByteOperations.unsafeWrap(chunk.flip()))
                        .build());
                }
            } catch (IOException e) {
                finished = true;
                requests.onError(e);
                result.completeExceptionally(e);
            }
        }

        @Override
        public void onNext(InstallSnapshotResponse value) {
        }

        @Override
        public synchronized void onError(Throwable error) {
            finished = true;
            result.completeExceptionally(error);
        }

        @Override
        public void onCompleted() {
            result.complete(null);
        }
    }
}
