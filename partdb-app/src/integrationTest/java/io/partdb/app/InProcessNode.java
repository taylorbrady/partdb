package io.partdb.app;

import io.partdb.client.ServerEndpoint;
import io.partdb.client.testing.NodeSpec;
import io.partdb.client.testing.TestClusterNode;
import io.partdb.server.PartDbServer;
import io.partdb.server.PartDbServerConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.UnaryOperator;

final class InProcessNode implements TestClusterNode {
    private final String nodeId;
    private final ServerEndpoint endpoint;
    private final PartDbServerConfig config;
    private PartDbServer server;

    InProcessNode(Path rootDir, NodeSpec spec) {
        this(rootDir, spec, UnaryOperator.identity());
    }

    InProcessNode(Path rootDir, NodeSpec spec, UnaryOperator<PartDbServerConfig> customize) {
        this.nodeId = spec.nodeId();
        this.endpoint = spec.grpcEndpoint();
        this.config = customize.apply(PartDbServerConfig.create(
            spec.nodeId(),
            spec.raftPeerAddresses(),
            rootDir.resolve(spec.nodeId()),
            spec.raftPort(),
            spec.grpcPort(),
            spec.adminPort()
        ));
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public ServerEndpoint endpoint() {
        return endpoint;
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    @Override
    public void start() throws IOException {
        if (server != null) {
            return;
        }
        var starting = new PartDbServer(config);
        starting.start();
        server = starting;
    }

    @Override
    public void stop() {
        if (server == null) {
            return;
        }
        try {
            server.close();
        } finally {
            server = null;
        }
    }
}
