package io.partdb.server.grpc;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.partdb.node.Node;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class GrpcServer implements AutoCloseable {
    private static final Duration SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(5);
    private static final Duration KEEPALIVE_PERMIT = Duration.ofSeconds(10);
    private static final int MAX_MESSAGE_BYTES = 16 << 20;

    private final Server server;

    public GrpcServer(Node node, int port) {
        this.server = NettyServerBuilder.forPort(port)
            .addService(new KeyValueService(node))
            .addService(new ClusterService(node))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .maxInboundMessageSize(MAX_MESSAGE_BYTES)
            .permitKeepAliveTime(KEEPALIVE_PERMIT.toMillis(), TimeUnit.MILLISECONDS)
            .permitKeepAliveWithoutCalls(true)
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(SHUTDOWN_GRACE_PERIOD.toMillis(), TimeUnit.MILLISECONDS)) {
                server.shutdownNow();
                server.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
