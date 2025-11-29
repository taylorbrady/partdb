package io.partdb.server.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.partdb.raft.RaftNode;
import io.partdb.server.Database;
import io.partdb.server.Lessor;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class KvServer implements AutoCloseable {
    private final Server server;
    private final KvServerConfig config;

    public KvServer(RaftNode raftNode, Database database, Lessor lessor, KvServerConfig config) {
        this.config = config;
        this.server = ServerBuilder.forPort(config.port())
            .addService(new KvServiceImpl(raftNode, database, lessor, config))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public int getPort() {
        return server.getPort();
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
