package io.partdb.app.testing;

import io.partdb.bytes.Bytes;
import io.partdb.client.Members;
import io.partdb.client.NodeStatus;
import io.partdb.client.PartDbClient;
import io.partdb.client.ReadConsistency;
import io.partdb.client.Role;

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
    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);

    @FunctionalInterface
    public interface NodeFactory<N extends TestClusterNode> {
        N create(NodeSpec spec) throws IOException;
    }

    @FunctionalInterface
    private interface Probe<T> {
        Optional<T> attempt() throws Exception;
    }

    private final LinkedHashMap<String, N> nodes = new LinkedHashMap<>();
    private final Map<String, PartDbClient> probes = new LinkedHashMap<>();
    private PartDbClient client;

    private TestCluster() {
    }

    public static <N extends TestClusterNode> TestCluster<N> create(int size, NodeFactory<N> factory) throws IOException {
        if (size < 1) {
            throw new IllegalArgumentException("size must be at least 1");
        }
        var peers = new LinkedHashMap<String, String>();
        if (size > 1) {
            for (int i = 1; i <= size; i++) {
                peers.put("node" + i, "localhost:" + freePort());
            }
        }
        var cluster = new TestCluster<N>();
        for (int i = 1; i <= size; i++) {
            var spec = new NodeSpec("node" + i, freePort(), freePort(), peers);
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
        var reverse = new ArrayList<>(nodes.values());
        Collections.reverse(reverse);
        var failures = new ArrayList<Throwable>();
        for (N node : reverse) {
            try {
                node.stop();
            } catch (RuntimeException | Error e) {
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            var error = new AssertionError("failed to stop " + failures.size() + " node(s)", failures.getFirst());
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
            throw new IllegalArgumentException("unknown node: " + nodeId);
        }
        return node;
    }

    public List<String> startedNodeIds() {
        return nodes.values().stream().filter(TestClusterNode::isStarted).map(TestClusterNode::nodeId).toList();
    }

    public String endpoint(String nodeId) {
        return node(nodeId).endpoint().toString();
    }

    public String endpoints() {
        return String.join(",", nodes.values().stream().map(node -> node.endpoint().toString()).toList());
    }

    public synchronized PartDbClient client() {
        if (client == null) {
            client = PartDbClient.builder().endpoints(nodes.values().stream().map(TestClusterNode::endpoint).toList()).build();
        }
        return client;
    }

    public NodeStatus status(String nodeId) {
        return probe(nodeId).cluster().status(node(nodeId).endpoint());
    }

    public Members members(String nodeId) {
        return probe(nodeId).cluster().members();
    }

    public N awaitStableLeader(Duration timeout) throws InterruptedException {
        return awaitStableLeader(timeout, null);
    }

    public N awaitStableLeaderExcluding(String excludedNodeId, Duration timeout) throws InterruptedException {
        return awaitStableLeader(timeout, excludedNodeId);
    }

    public NodeStatus awaitStatusLeader(String nodeId, String leaderId, Duration timeout) throws InterruptedException {
        return await(timeout, "status on " + nodeId + " to report leader " + leaderId, () -> {
            var status = status(nodeId);
            return status.leaderId().filter(leaderId::equals).map(_ -> status);
        });
    }

    public Members awaitMembersLeader(String nodeId, String leaderId, Duration timeout) throws InterruptedException {
        return await(timeout, "members on " + nodeId + " to report leader " + leaderId, () -> {
            var members = members(nodeId);
            return members.leaderId().filter(leaderId::equals).map(_ -> members);
        });
    }

    public void awaitCaughtUp(String nodeId, Duration timeout) throws InterruptedException {
        var leader = awaitStableLeader(timeout);
        long target = status(leader.nodeId()).commitIndex();
        await(timeout, nodeId + " to apply through " + target, () -> {
            var status = status(nodeId);
            return status.appliedIndex() >= target ? Optional.of(status) : Optional.empty();
        });
    }

    public void awaitNodeValue(String nodeId, String key, String expected, Duration timeout) throws InterruptedException {
        var value = Bytes.utf8(expected);
        await(timeout, nodeId + " to observe " + key + "=" + expected, () ->
            probe(nodeId).kv().get(Bytes.utf8(key), ReadConsistency.LOCAL).filter(entry -> entry.value().equals(value)));
    }

    @Override
    public void close() {
        synchronized (this) {
            if (client != null) {
                client.close();
            }
            probes.values().forEach(PartDbClient::close);
            probes.clear();
        }
        stopAll();
    }

    private synchronized PartDbClient probe(String nodeId) {
        return probes.computeIfAbsent(nodeId, id -> PartDbClient.builder()
            .endpoints(List.of(node(id).endpoint()))
            .requestTimeout(PROBE_TIMEOUT)
            .build());
    }

    private N awaitStableLeader(Duration timeout, String excluded) throws InterruptedException {
        if (startedNodeIds().isEmpty()) {
            throw new IllegalStateException("no started nodes");
        }
        return await(timeout, "a stable leader", () -> {
            var started = startedNodeIds();
            for (String nodeId : started) {
                node(nodeId).checkAlive();
            }
            var statuses = new LinkedHashMap<String, NodeStatus>();
            for (String nodeId : started) {
                statuses.put(nodeId, status(nodeId));
            }
            var leaders = statuses.entrySet().stream()
                .filter(entry -> entry.getValue().role() == Role.LEADER)
                .map(Map.Entry::getKey)
                .toList();
            if (leaders.size() != 1 || leaders.getFirst().equals(excluded)) {
                return Optional.empty();
            }
            String leader = leaders.getFirst();
            boolean agreed = statuses.values().stream().allMatch(status -> status.leaderId().filter(leader::equals).isPresent());
            return agreed ? Optional.of(node(leader)) : Optional.empty();
        });
    }

    private static <T> T await(Duration timeout, String description, Probe<T> probe) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                var result = probe.attempt();
                if (result.isPresent()) {
                    return result.get();
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (AssertionError e) {
                throw e;
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }
        throw new AssertionError("timed out waiting for " + description, last);
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
