package io.partdb.server;

import io.partdb.node.PartDbNode;
import io.partdb.server.grpc.GrpcServer;
import io.partdb.server.peer.GrpcRaftTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final Deque<AutoCloseable> started = new ArrayDeque<>();
    private boolean startCalled;

    public PartDbServer(PartDbServerConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    public void start() throws IOException {
        if (startCalled) {
            throw new IllegalStateException("PartDB server already started");
        }
        startCalled = true;
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Starting PartDB server");
        try {
            var transport = new GrpcRaftTransport(
                config.nodeConfig().raft().clusterId(), config.nodeId(), config.raftPort(), config.raftPeerEndpoints());
            started.push(transport);
            PartDbNode node = PartDbNode.open(config.nodeConfig(), transport);
            started.push(node);
            var jmxRegistrations = new JmxRegistrations(node);
            started.push(jmxRegistrations);
            var grpcServer = new GrpcServer(node, config.grpcPort());
            started.push(grpcServer);
            grpcServer.start();
            var adminHttpServer = new AdminHttpServer(node, config.adminPort());
            started.push(adminHttpServer);
            adminHttpServer.start();
            jmxRegistrations.register();
            log.atInfo()
                .addKeyValue("nodeId", config.nodeId())
                .addKeyValue("grpcPort", config.grpcPort())
                .addKeyValue("raftPort", config.raftPort())
                .addKeyValue("adminPort", config.adminPort())
                .log("PartDB server started");
        } catch (Exception e) {
            close();
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Failed to start PartDB server", e);
        }
    }

    @Override
    public void close() {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Shutting down PartDB server");
        RuntimeException failure = null;
        while (!started.isEmpty()) {
            try {
                started.pop().close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e instanceof RuntimeException re ? re : new IllegalStateException(e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("PartDB server shut down");
        if (failure != null) {
            throw failure;
        }
    }
}
