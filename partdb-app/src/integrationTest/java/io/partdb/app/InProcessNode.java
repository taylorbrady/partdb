package io.partdb.app;

import io.partdb.app.testing.NodeSpec;
import io.partdb.app.testing.TestClusterNode;
import io.partdb.client.ServerEndpoint;
import io.partdb.raft.runtime.SnapshotPolicy;
import io.partdb.server.PartDbServer;
import io.partdb.server.ServerConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

final class InProcessNode implements TestClusterNode {
    private final NodeSpec spec;
    private final ServerConfig config;
    private PartDbServer server;

    InProcessNode(Path root, NodeSpec spec) {
        this(root, spec, SnapshotPolicy.DEFAULT);
    }

    InProcessNode(Path root, NodeSpec spec, SnapshotPolicy snapshots) {
        this.spec = spec;
        this.config = ServerConfig.create(
            spec.nodeId(), spec.peers(), root.resolve(spec.nodeId()), spec.clientPort(), spec.adminPort(), Optional.empty(), snapshots);
    }

    @Override
    public String nodeId() {
        return spec.nodeId();
    }

    @Override
    public ServerEndpoint endpoint() {
        return spec.endpoint();
    }

    @Override
    public boolean isStarted() {
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
