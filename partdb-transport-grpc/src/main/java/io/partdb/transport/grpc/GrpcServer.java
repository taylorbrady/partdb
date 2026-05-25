package io.partdb.transport.grpc;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.partdb.node.PartDbNode;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class GrpcServer implements AutoCloseable {
    private final Server server;
    private final GrpcServerConfig config;

    public GrpcServer(PartDbNode node, int port) {
        this(node, GrpcServerConfig.defaultConfig(port));
    }

    GrpcServer(PartDbNode node, GrpcServerConfig config) {
        this.config = config;
        this.server = NettyServerBuilder.forPort(config.port())
            .addService(new KvServiceImpl(node, config))
            .addService(new ClusterServiceImpl(node))
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
            if (!server.awaitTermination(config.shutdownGracePeriod().toMillis(), TimeUnit.MILLISECONDS)) {
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
