package io.partdb.server;

import io.partdb.node.Node;
import io.partdb.node.NodeException;
import io.partdb.raft.Role;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.grpc.GrpcServer;
import io.partdb.server.peer.GrpcRaftTransport;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = System.getLogger(PartDbServer.class.getName());
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
        long started = System.nanoTime();
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
            var grpc = new GrpcServer(node, config.clientPort());
            resources.push(grpc);
            grpc.start();
            var admin = new AdminHttpServer(node, config.adminPort());
            resources.push(admin);
            admin.start();
            log.log(Level.INFO, "PartDB server started",
                entry("clusterId", config.clusterId()),
                entry("clientPort", config.clientPort()),
                entry("adminPort", config.adminPort()),
                entry("raftPort", config.raftPort()),
                entry("peers", peers(config)),
                entry("logRetentionEntries", config.retention().entries()),
                entry("logRetentionBytes", config.retention().bytes()),
                entry("duration", Duration.ofNanos(System.nanoTime() - started)));
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
        log.log(Level.INFO, "Shutting down PartDB server");
        admin.drain();
        handOffLeadership();
        var failure = closeAll(resources);
        log.log(Level.INFO, "PartDB server stopped");
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
            log.log(Level.INFO, "Leadership hand-off skipped", entry("reason", e.getMessage()));
        }
    }

    private static String peers(ServerConfig config) {
        if (config.peers().isEmpty()) {
            return "none";
        }
        return config.peers().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(peer -> peer.getKey() + "=" + peer.getValue().host() + ":" + peer.getValue().port())
            .collect(Collectors.joining(","));
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
