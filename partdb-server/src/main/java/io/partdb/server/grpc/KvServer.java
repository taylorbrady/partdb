package io.partdb.server.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.partdb.node.KvStore;
import io.partdb.node.Lessor;
import io.partdb.node.Proposer;
import io.partdb.node.raft.RaftNode;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class KvServer implements AutoCloseable {
    private final Server server;
    private final KvServerConfig config;

    public KvServer(
        Proposer proposer,
        Lessor lessor,
        KvStore kvStore,
        RaftNode raftNode,
        Map<String, String> peerAddresses,
        String selfAddress,
        KvServerConfig config
    ) {
        this.config = config;
        this.server = ServerBuilder.forPort(config.port())
            .addService(new KvServiceImpl(proposer, lessor, kvStore, config))
            .addService(new ClusterServiceImpl(raftNode, peerAddresses, selfAddress))
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
