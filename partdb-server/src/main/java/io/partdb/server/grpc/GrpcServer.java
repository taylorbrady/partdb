package io.partdb.server.grpc;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.partdb.node.PartDbNode;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class GrpcServer implements AutoCloseable {
    private static final Duration SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(5);

    private final Server server;

    public GrpcServer(PartDbNode node, int port) {
        this.server = NettyServerBuilder.forPort(port)
            .addService(new KvService(node))
            .addService(new ClusterService(node))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
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
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Server did not terminate");
                }
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
