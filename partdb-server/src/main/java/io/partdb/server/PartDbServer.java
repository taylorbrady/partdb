package io.partdb.server;

import io.partdb.node.Node;
import io.partdb.node.NodeException;
import io.partdb.raft.Role;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.grpc.GrpcServer;
import io.partdb.server.peer.GrpcRaftTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);
    private static final Duration HAND_OFF_TIMEOUT = Duration.ofSeconds(2);

    private final ServerConfig config;
    private final Node node;
    private final AdminHttpServer admin;
    private final Deque<AutoCloseable> resources;
    private boolean closed;

    private PartDbServer(ServerConfig config, Node node, AdminHttpServer admin, Deque<AutoCloseable> resources) {
        this.config = config;
        this.node = node;
        this.admin = admin;
        this.resources = resources;
    }

    public static PartDbServer start(ServerConfig config) throws IOException {
        Objects.requireNonNull(config, "config must not be null");
        log.atInfo().addKeyValue("nodeId", config.nodeId()).log("Starting PartDB server");
        var resources = new ArrayDeque<AutoCloseable>();
        try {
            RaftTransport transport = RaftTransport.standalone();
            if (!config.peers().isEmpty()) {
                var peers = new GrpcRaftTransport(config.clusterId(), config.nodeId(), config.raftPort().orElseThrow(), config.peers());
                resources.push(peers);
                transport = peers;
            }
            var node = Node.open(config.node(), transport);
            resources.push(node);
            var jmx = new JmxRegistrations(node);
            resources.push(jmx);
            var grpc = new GrpcServer(node, config.clientPort());
            resources.push(grpc);
            grpc.start();
            var admin = new AdminHttpServer(node, config.adminPort());
            resources.push(admin);
            admin.start();
            jmx.register();
            log.atInfo()
                .addKeyValue("nodeId", config.nodeId())
                .addKeyValue("clientPort", config.clientPort())
                .addKeyValue("raftPort", config.raftPort().orElse(null))
                .addKeyValue("adminPort", config.adminPort())
                .log("PartDB server started");
            return new PartDbServer(config, node, admin, resources);
        } catch (IOException | RuntimeException e) {
            var failure = closeAll(resources);
            if (failure != null) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        log.atInfo().addKeyValue("nodeId", config.nodeId()).log("Shutting down PartDB server");
        admin.drain();
        handOffLeadership();
        var failure = closeAll(resources);
        log.atInfo().addKeyValue("nodeId", config.nodeId()).log("PartDB server shut down");
        if (failure != null) {
            throw failure;
        }
    }

    private void handOffLeadership() {
        if (node.status().role() != Role.LEADER) {
            return;
        }
        try {
            node.stepDown(HAND_OFF_TIMEOUT);
        } catch (NodeException e) {
            log.atInfo().addKeyValue("reason", e.getMessage()).log("Leadership hand-off skipped");
        }
    }

    private static RuntimeException closeAll(Deque<AutoCloseable> resources) {
        RuntimeException failure = null;
        while (!resources.isEmpty()) {
            try {
                resources.pop().close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        return failure;
    }
}
