package io.partdb.client.testing;

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

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class TestCluster<N extends TestClusterNode> implements AutoCloseable {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @FunctionalInterface
    public interface NodeFactory<N extends TestClusterNode> {
        N create(NodeSpec spec) throws IOException;
    }

    @FunctionalInterface
    private interface Probe<T> {
        Optional<T> attempt() throws Exception;
    }

    private final LinkedHashMap<String, N> nodes = new LinkedHashMap<>();

    private TestCluster() {
    }

    public static <N extends TestClusterNode> TestCluster<N> create(int nodeCount, NodeFactory<N> factory)
        throws IOException {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("nodeCount must be at least 1");
        }

        var raftPorts = new LinkedHashMap<String, Integer>();
        for (int i = 1; i <= nodeCount; i++) {
            raftPorts.put("node" + i, freePort());
        }
        var raftPeerAddresses = new LinkedHashMap<String, String>();
        raftPorts.forEach((nodeId, port) -> raftPeerAddresses.put(nodeId, "localhost:" + port));

        var cluster = new TestCluster<N>();
        for (var entry : raftPorts.entrySet()) {
            var spec = new NodeSpec(entry.getKey(), entry.getValue(), freePort(), freePort(), raftPeerAddresses);
            cluster.nodes.put(spec.nodeId(), factory.create(spec));
        }
        return cluster;
    }

    public void startAll() throws IOException {
        for (N node : nodes.values()) {
            node.start();
        }
    }

    public void stopAll() {
        var reverseOrder = new ArrayList<>(nodes.values());
        Collections.reverse(reverseOrder);
        var failures = new ArrayList<Throwable>();
        for (N node : reverseOrder) {
            try {
                node.stop();
            } catch (RuntimeException | Error e) {
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            var error = new AssertionError("Failed to stop " + failures.size() + " node(s)", failures.getFirst());
            failures.stream().skip(1).forEach(error::addSuppressed);
            throw error;
        }
    }

    public void startNode(String nodeId) throws IOException {
        node(nodeId).start();
    }

    public void stopNode(String nodeId) {
        node(nodeId).stop();
    }

    public N node(String nodeId) {
        N node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node: " + nodeId);
        }
        return node;
    }

    public List<N> runningNodes() {
        return nodes.values().stream().filter(TestClusterNode::isRunning).toList();
    }

    public List<String> runningNodeIds() {
        return runningNodes().stream().map(TestClusterNode::nodeId).toList();
    }

    public String grpcAddress(String nodeId) {
        return node(nodeId).endpoint().toString();
    }

    public KvClient newKvClient() {
        var endpoints = runningNodes().stream()
            .map(TestClusterNode::endpoint)
            .toArray(ServerEndpoint[]::new);
        return new KvClient(KvClientConfig.defaultConfig(endpoints));
    }

    public KvClient newKvClient(String nodeId) {
        return new KvClient(KvClientConfig.defaultConfig(node(nodeId).endpoint()));
    }

    public ClusterStatus status(String nodeId) throws Exception {
        try (var client = newClusterClient(nodeId)) {
            return client.status().get();
        }
    }

    public ClusterMembership membership(String nodeId) throws Exception {
        try (var client = newClusterClient(nodeId)) {
            return client.membership().get();
        }
    }

    public N awaitStableLeader(Duration timeout) throws InterruptedException {
        return awaitStableLeader(timeout, null);
    }

    public N awaitStableLeaderExcluding(String excludedNodeId, Duration timeout) throws InterruptedException {
        return awaitStableLeader(timeout, excludedNodeId);
    }

    public ClusterStatus awaitStatusLeader(String nodeId, String expectedLeaderId, Duration timeout)
        throws InterruptedException {
        return await(timeout, "status on " + nodeId + " to report leader " + expectedLeaderId, () -> {
            ClusterStatus status = status(nodeId);
            return reportsLeader(status, expectedLeaderId) ? Optional.of(status) : Optional.empty();
        });
    }

    public ClusterMembership awaitMembershipLeader(String nodeId, String expectedLeaderId, Duration timeout)
        throws InterruptedException {
        return await(timeout, "membership on " + nodeId + " to report leader " + expectedLeaderId, () -> {
            ClusterMembership membership = membership(nodeId);
            boolean leaderFlagged = membership.members().stream()
                .filter(ClusterMember::leader)
                .map(ClusterMember::nodeId)
                .toList()
                .equals(List.of(expectedLeaderId));
            boolean leaderReported = membership.leaderId().map(expectedLeaderId::equals).orElse(false);
            return leaderFlagged && leaderReported ? Optional.of(membership) : Optional.empty();
        });
    }

    public ClusterMembership awaitMembershipSize(String nodeId, int expectedMembers, Duration timeout)
        throws InterruptedException {
        return await(timeout, "membership size " + expectedMembers + " on " + nodeId, () -> {
            ClusterMembership membership = membership(nodeId);
            return membership.members().size() == expectedMembers ? Optional.of(membership) : Optional.empty();
        });
    }

    public void awaitNodeValue(String nodeId, String key, String expectedValue, Duration timeout)
        throws InterruptedException {
        Bytes expected = Bytes.utf8(expectedValue);
        await(timeout, nodeId + " to observe " + key + "=" + expectedValue, () -> {
            try (var client = newKvClient(nodeId)) {
                return client.get(Bytes.utf8(key), ReadConsistency.STALE).get().filter(expected::equals);
            }
        });
    }

    @Override
    public void close() {
        stopAll();
    }

    private N awaitStableLeader(Duration timeout, String excludedNodeId) throws InterruptedException {
        if (runningNodes().isEmpty()) {
            throw new IllegalStateException("No running nodes");
        }
        return await(timeout, "stable leader", () -> {
            var running = runningNodes();
            for (N node : running) {
                node.assertStillRunning();
            }

            var statuses = new LinkedHashMap<String, ClusterStatus>();
            for (N node : running) {
                statuses.put(node.nodeId(), status(node.nodeId()));
            }

            var leaders = statuses.entrySet().stream()
                .filter(entry -> entry.getValue().running() && entry.getValue().role() == ClusterNodeRole.LEADER)
                .map(Map.Entry::getKey)
                .toList();
            if (leaders.size() != 1 || leaders.getFirst().equals(excludedNodeId)) {
                return Optional.empty();
            }

            String leaderId = leaders.getFirst();
            boolean agreed = statuses.values().stream().allMatch(status -> reportsLeader(status, leaderId));
            return agreed ? Optional.of(node(leaderId)) : Optional.empty();
        });
    }

    private static boolean reportsLeader(ClusterStatus status, String leaderId) {
        return status.running() && status.leaderId().map(leaderId::equals).orElse(false);
    }

    private ClusterClient newClusterClient(String nodeId) {
        return new ClusterClient(ClusterClientConfig.defaultConfig(node(nodeId).endpoint()));
    }

    private static <T> T await(Duration timeout, String description, Probe<T> probe) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Exception lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try {
                Optional<T> result = probe.attempt();
                if (result.isPresent()) {
                    return result.get();
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError("Timed out waiting for " + description, lastFailure);
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
