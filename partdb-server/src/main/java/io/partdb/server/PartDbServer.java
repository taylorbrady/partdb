package io.partdb.server;

import io.partdb.node.Node;
import io.partdb.raft.NodeId;
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
    private final Deque<AutoCloseable> started = new ArrayDeque<>();
    private Node node;
    private AdminHttpServer admin;
    private boolean startCalled;

    public PartDbServer(ServerConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    public void start() throws IOException {
        if (startCalled) {
            throw new IllegalStateException("the server is already started");
        }
        startCalled = true;
        log.atInfo().addKeyValue("nodeId", config.nodeId()).log("Starting PartDB server");
        try {
            RaftTransport transport = RaftTransport.standalone();
            if (!config.peers().isEmpty()) {
                var grpcTransport = new GrpcRaftTransport(
                    config.node().raft().clusterId(), config.nodeId(), config.raftPort().orElseThrow(), config.peers());
                started.push(grpcTransport);
                transport = grpcTransport;
            }
            node = Node.open(config.node(), transport);
            started.push(node);
            var jmx = new JmxRegistrations(node);
            started.push(jmx);
            var grpc = new GrpcServer(node, config.clientPort());
            started.push(grpc);
            grpc.start();
            admin = new AdminHttpServer(node, config.adminPort());
            started.push(admin);
            admin.start();
            jmx.register();
            log.atInfo()
                .addKeyValue("nodeId", config.nodeId())
                .addKeyValue("clientPort", config.clientPort())
                .addKeyValue("raftPort", config.raftPort().orElse(null))
                .addKeyValue("adminPort", config.adminPort())
                .log("PartDB server started");
        } catch (Exception e) {
            close();
            if (e instanceof IOException io) {
                throw io;
            }
            if (e instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException("failed to start the PartDB server", e);
        }
    }

    @Override
    public void close() {
        log.atInfo().addKeyValue("nodeId", config.nodeId()).log("Shutting down PartDB server");
        if (admin != null) {
            admin.drain();
        }
        handOffLeadership();
        RuntimeException failure = null;
        while (!started.isEmpty()) {
            try {
                started.pop().close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        log.atInfo().addKeyValue("nodeId", config.nodeId()).log("PartDB server shut down");
        if (failure != null) {
            throw failure;
        }
    }

    private void handOffLeadership() {
        if (node == null || node.cluster().status().role() != Role.LEADER) {
            return;
        }
        NodeId self = config.nodeId();
        node.cluster().membership().voters().stream()
            .filter(voter -> !voter.equals(self))
            .findFirst()
            .ifPresent(target -> {
                try {
                    node.cluster().transferLeadership(target, HAND_OFF_TIMEOUT);
                } catch (RuntimeException e) {
                    log.atInfo().addKeyValue("target", target).addKeyValue("reason", e.getMessage()).log("Leadership hand-off skipped");
                }
            });
    }
}
