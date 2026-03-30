package io.partdb.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MultiNodeClusterIntegrationTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    Path tempDir;

    @Test
    void leaderFailoverPreservesWritesAndRecoveredLeaderCatchesUp() throws Exception {
        try (var cluster = MultiNodeClusterHarness.create(tempDir, 3)) {
            cluster.startAll();

            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            try (var client = cluster.newKvClient()) {
                client.put(bytes("alpha"), bytes("one")).get();
                assertEquals("one", decode(client.get(bytes("alpha")).get().orElseThrow()));
            }

            cluster.awaitNodeValue(initialLeader.nodeId(), "alpha", "one", CLUSTER_TIMEOUT);

            cluster.stopNode(initialLeader.nodeId());

            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            assertNotEquals(initialLeader.nodeId(), newLeader.nodeId());

            try (var client = cluster.newKvClient()) {
                client.put(bytes("beta"), bytes("two")).get();
                assertEquals("two", decode(client.get(bytes("beta")).get().orElseThrow()));
            }

            cluster.awaitNodeValue(newLeader.nodeId(), "beta", "two", CLUSTER_TIMEOUT);

            cluster.startNode(initialLeader.nodeId());

            var stableLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var membership = cluster.awaitMembershipSize(stableLeader.nodeId(), 3, CLUSTER_TIMEOUT);
            assertEquals(3, membership.members().size());

            cluster.awaitNodeValue(initialLeader.nodeId(), "alpha", "one", CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(initialLeader.nodeId(), "beta", "two", CLUSTER_TIMEOUT);
        }
    }

    @Test
    void restartedFollowerCatchesUpOnMissedWrites() throws Exception {
        try (var cluster = MultiNodeClusterHarness.create(tempDir, 3)) {
            cluster.startAll();

            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String followerId = cluster.runningNodeIds().stream()
                .filter(nodeId -> !nodeId.equals(leader.nodeId()))
                .findFirst()
                .orElseThrow();

            cluster.stopNode(followerId);

            try (var client = cluster.newKvClient()) {
                client.put(bytes("gamma"), bytes("three")).get();
                client.put(bytes("delta"), bytes("four")).get();
            }

            cluster.startNode(followerId);
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            cluster.awaitNodeValue(followerId, "gamma", "three", CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(followerId, "delta", "four", CLUSTER_TIMEOUT);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String decode(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
