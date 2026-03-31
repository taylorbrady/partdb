package io.partdb.app;

import io.partdb.bytes.Bytes;
import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterClientConfig;
import io.partdb.client.ClusterMember;
import io.partdb.client.ClusterMembership;
import io.partdb.client.ClusterNodeRole;
import io.partdb.client.ClusterStatus;
import io.partdb.client.KvClient;
import io.partdb.client.KvClientConfig;
import io.partdb.client.ReadConsistency;
import io.partdb.client.ServerEndpoint;
import io.partdb.transport.grpc.PartDbServer;
import io.partdb.transport.grpc.PartDbServerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class MultiNodeClusterHarness implements AutoCloseable {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    private final LinkedHashMap<String, NodeHandle> nodes;

    private MultiNodeClusterHarness(LinkedHashMap<String, NodeHandle> nodes) {
        this.nodes = nodes;
    }

    static MultiNodeClusterHarness create(Path rootDir, int nodeCount) throws IOException {
        if (nodeCount < 3) {
            throw new IllegalArgumentException("nodeCount must be at least 3");
        }

        record NodeSpec(String nodeId, int raftPort, int grpcPort, int adminPort) {}

        var specs = new ArrayList<NodeSpec>(nodeCount);
        for (int i = 1; i <= nodeCount; i++) {
            specs.add(new NodeSpec("node" + i, freePort(), freePort(), freePort()));
        }

        var raftPeerAddresses = new LinkedHashMap<String, String>();
        for (NodeSpec spec : specs) {
            raftPeerAddresses.put(spec.nodeId(), "localhost:" + spec.raftPort());
        }

        var nodes = new LinkedHashMap<String, NodeHandle>();
        for (NodeSpec spec : specs) {
            var config = PartDbServerConfig.create(
                spec.nodeId(),
                raftPeerAddresses,
                rootDir.resolve(spec.nodeId()),
                spec.raftPort(),
                spec.grpcPort(),
                spec.adminPort()
            );
            nodes.put(spec.nodeId(), new NodeHandle(spec.nodeId(), spec.raftPort(), spec.grpcPort(), config));
        }

        return new MultiNodeClusterHarness(nodes);
    }

    void startAll() throws IOException {
        for (NodeHandle node : nodes.values()) {
            node.start();
        }
    }

    void stopAll() {
        var reverseOrder = new ArrayList<>(nodes.values());
        java.util.Collections.reverse(reverseOrder);
        for (NodeHandle node : reverseOrder) {
            node.stop();
        }
    }

    void startNode(String nodeId) throws IOException {
        node(nodeId).start();
    }

    void stopNode(String nodeId) {
        node(nodeId).stop();
    }

    NodeHandle awaitStableLeader(Duration timeout) throws Exception {
        return awaitStableLeader(timeout, null);
    }

    NodeHandle awaitStableLeaderExcluding(String excludedNodeId, Duration timeout) throws Exception {
        Objects.requireNonNull(excludedNodeId, "excludedNodeId must not be null");
        return awaitStableLeader(timeout, excludedNodeId);
    }

    ClusterMembership awaitMembershipSize(String nodeId, int expectedMembers, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try (var client = newClusterClient(nodeId)) {
                var membership = client.membership().get();
                if (membership.members().size() == expectedMembers) {
                    return membership;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError("Timed out waiting for membership size " + expectedMembers, lastFailure);
    }

    ClusterStatus awaitStatusLeader(String nodeId, String expectedLeaderId, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try {
                ClusterStatus status = status(nodeId);
                if (status.running() && effectiveLeaderId(status).map(expectedLeaderId::equals).orElse(false)) {
                    return status;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError(
            "Timed out waiting for status on " + nodeId + " to report leader " + expectedLeaderId,
            lastFailure
        );
    }

    ClusterMembership awaitMembershipLeader(String nodeId, String expectedLeaderId, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try {
                ClusterMembership membership = membership(nodeId);
                long leaderCount = membership.members().stream().filter(ClusterMember::leader).count();
                if (leaderCount == 1
                    && membership.leaderId().map(expectedLeaderId::equals).orElse(false)
                    && membership.members().stream().anyMatch(member -> member.nodeId().equals(expectedLeaderId) && member.leader())) {
                    return membership;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError(
            "Timed out waiting for membership on " + nodeId + " to report leader " + expectedLeaderId,
            lastFailure
        );
    }

    ClusterStatus status(String nodeId) throws Exception {
        try (var client = newClusterClient(nodeId)) {
            return client.status().get();
        }
    }

    ClusterMembership membership(String nodeId) throws Exception {
        try (var client = newClusterClient(nodeId)) {
            return client.membership().get();
        }
    }

    void awaitNodeValue(String nodeId, String key, String expectedValue, Duration timeout) throws Exception {
        Bytes expectedBytes = bytes(expectedValue);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try (var client = newKvClient(nodeId)) {
                var value = client.get(bytes(key), ReadConsistency.STALE).get();
                if (value.isPresent() && value.get().equals(expectedBytes)) {
                    return;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError(
            "Timed out waiting for " + nodeId + " to observe " + key + "=" + expectedValue,
            lastFailure
        );
    }

    KvClient newKvClient() {
        var endpoints = runningNodes().stream()
            .map(NodeHandle::grpcEndpoint)
            .toArray(ServerEndpoint[]::new);
        return new KvClient(KvClientConfig.defaultConfig(endpoints));
    }

    KvClient newKvClient(String nodeId) {
        return new KvClient(KvClientConfig.defaultConfig(node(nodeId).grpcEndpoint()));
    }

    List<String> runningNodeIds() {
        return runningNodes().stream()
            .map(NodeHandle::nodeId)
            .toList();
    }

    String raftAddress(String nodeId) {
        return "localhost:" + node(nodeId).raftPort();
    }

    String grpcAddress(String nodeId) {
        return node(nodeId).grpcEndpoint().toString();
    }

    CommandResult runCommand(String... args) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        try (
            PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
            PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
        ) {
            int exitCode = PartDbApp.run(args, out, err);
            return new CommandResult(
                exitCode,
                outBytes.toString(StandardCharsets.UTF_8),
                errBytes.toString(StandardCharsets.UTF_8)
            );
        }
    }

    @Override
    public void close() {
        stopAll();
    }

    private NodeHandle awaitStableLeader(Duration timeout, String excludedNodeId) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            var runningNodes = runningNodes();
            if (runningNodes.isEmpty()) {
                throw new IllegalStateException("No running nodes");
            }

            var statuses = new LinkedHashMap<String, ClusterStatus>();
            boolean allAvailable = true;

            for (NodeHandle node : runningNodes) {
                try (var client = newClusterClient(node.nodeId())) {
                    statuses.put(node.nodeId(), client.status().get());
                } catch (Exception e) {
                    lastFailure = e;
                    allAvailable = false;
                    break;
                }
            }

            if (allAvailable) {
                var leaders = statuses.entrySet().stream()
                    .filter(entry -> entry.getValue().role() == ClusterNodeRole.LEADER && entry.getValue().running())
                    .map(Map.Entry::getKey)
                    .toList();

                if (leaders.size() == 1) {
                    String leaderId = leaders.get(0);
                    if ((excludedNodeId == null || !excludedNodeId.equals(leaderId))
                        && statuses.values().stream().allMatch(status ->
                            status.running() && effectiveLeaderId(status)
                                .map(leaderId::equals)
                                .orElse(false))) {
                        return node(leaderId);
                    }
                }
            }

            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError("Timed out waiting for stable leader", lastFailure);
    }

    private ClusterClient newClusterClient(String nodeId) {
        return new ClusterClient(ClusterClientConfig.defaultConfig(node(nodeId).grpcEndpoint()));
    }

    private List<NodeHandle> runningNodes() {
        return nodes.values().stream()
            .filter(NodeHandle::isRunning)
            .toList();
    }

    private NodeHandle node(String nodeId) {
        NodeHandle node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node: " + nodeId);
        }
        return node;
    }

    private static Optional<String> effectiveLeaderId(ClusterStatus status) {
        if (status.leaderId().isPresent()) {
            return status.leaderId();
        }
        if (status.role() == ClusterNodeRole.LEADER) {
            return Optional.of(status.nodeId());
        }
        return Optional.empty();
    }

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static final class NodeHandle {
        private final String nodeId;
        private final int raftPort;
        private final int grpcPort;
        private final ServerEndpoint grpcEndpoint;
        private final PartDbServerConfig config;
        private PartDbServer server;

        NodeHandle(String nodeId, int raftPort, int grpcPort, PartDbServerConfig config) {
            this.nodeId = nodeId;
            this.raftPort = raftPort;
            this.grpcPort = grpcPort;
            this.grpcEndpoint = new ServerEndpoint("localhost", grpcPort);
            this.config = config;
        }

        String nodeId() {
            return nodeId;
        }

        int raftPort() {
            return raftPort;
        }

        int grpcPort() {
            return grpcPort;
        }

        ServerEndpoint grpcEndpoint() {
            return grpcEndpoint;
        }

        boolean isRunning() {
            return server != null;
        }

        void start() throws IOException {
            if (server != null) {
                return;
            }
            server = new PartDbServer(config);
            server.start();
        }

        void stop() {
            if (server == null) {
                return;
            }
            server.close();
            server = null;
        }
    }

    record CommandResult(int exitCode, String stdout, String stderr) {}
}
