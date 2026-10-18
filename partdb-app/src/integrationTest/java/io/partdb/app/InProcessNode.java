package io.partdb.app;

import io.partdb.app.testing.NodeSpec;
import io.partdb.app.testing.TestClusterNode;
import io.partdb.client.ServerEndpoint;
import io.partdb.server.PartDbServer;
import io.partdb.server.ServerConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

final class InProcessNode implements TestClusterNode {
    private final NodeSpec spec;
    private final ServerConfig config;
    private PartDbServer server;

    InProcessNode(Path root, NodeSpec spec) {
        this(root, spec, _ -> {});
    }

    InProcessNode(Path root, NodeSpec spec, Consumer<ServerConfig.Builder> settings) {
        this.spec = spec;
        var builder = ServerConfig.builder(spec.nodeId(), root.resolve(spec.nodeId())).clientPort(spec.clientPort()).adminPort(spec.adminPort());
        spec.peers().forEach(builder::peer);
        settings.accept(builder);
        this.config = builder.build();
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
        server = PartDbServer.start(config);
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
