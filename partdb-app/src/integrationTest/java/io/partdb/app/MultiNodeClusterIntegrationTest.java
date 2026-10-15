package io.partdb.app;

import io.partdb.bytes.Bytes;
import io.partdb.client.testing.TestCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MultiNodeClusterIntegrationTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(10);
    private static final int REPLAY_WRITE_COUNT = 32;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Test
    void threeNodeClusterSupportsFailoverRestartCatchupAndScanThroughPublicClient() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            try (var client = cluster.newKvClient()) {
                client.put(Bytes.utf8("proof/a"), Bytes.utf8("one")).get();
                client.put(Bytes.utf8("proof/b"), Bytes.utf8("two")).get();

                assertEquals("one", client.get(Bytes.utf8("proof/a")).get().orElseThrow().utf8());
                assertEquals("two", client.get(Bytes.utf8("proof/b")).get().orElseThrow().utf8());
            }

            cluster.stopNode(initialLeader.nodeId());

            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            assertNotEquals(initialLeader.nodeId(), newLeader.nodeId());

            try (var client = cluster.newKvClient()) {
                assertEquals("one", client.get(Bytes.utf8("proof/a")).get().orElseThrow().utf8());
                client.put(Bytes.utf8("proof/c"), Bytes.utf8("three")).get();
                assertEquals("three", client.get(Bytes.utf8("proof/c")).get().orElseThrow().utf8());
            }

            cluster.startNode(initialLeader.nodeId());
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.awaitMembershipSize(initialLeader.nodeId(), 3, CLUSTER_TIMEOUT);

            cluster.awaitNodeValue(initialLeader.nodeId(), "proof/a", "one", CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(initialLeader.nodeId(), "proof/b", "two", CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(initialLeader.nodeId(), "proof/c", "three", CLUSTER_TIMEOUT);

            try (var client = cluster.newKvClient()) {
                var scanned = new ArrayList<String>();
                try (var cursor = client.scan(Optional.of(Bytes.utf8("proof/")), Optional.of(Bytes.utf8("proof0"))).get()) {
                    while (cursor.hasNext()) {
                        var entry = cursor.next();
                        scanned.add(entry.key().utf8() + "=" + entry.value().utf8());
                    }
                }

                assertEquals(List.of("proof/a=one", "proof/b=two", "proof/c=three"), scanned);
            }
        }
    }

    @Test
    void leaderFailoverPreservesWritesAndRecoveredLeaderCatchesUp() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            try (var client = cluster.newKvClient()) {
                client.put(Bytes.utf8("alpha"), Bytes.utf8("one")).get();
                assertEquals("one", client.get(Bytes.utf8("alpha")).get().orElseThrow().utf8());
            }

            cluster.awaitNodeValue(initialLeader.nodeId(), "alpha", "one", CLUSTER_TIMEOUT);

            cluster.stopNode(initialLeader.nodeId());

            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            assertNotEquals(initialLeader.nodeId(), newLeader.nodeId());

            try (var client = cluster.newKvClient()) {
                client.put(Bytes.utf8("beta"), Bytes.utf8("two")).get();
                assertEquals("two", client.get(Bytes.utf8("beta")).get().orElseThrow().utf8());
            }

            cluster.awaitNodeValue(newLeader.nodeId(), "beta", "two", CLUSTER_TIMEOUT);

            cluster.startNode(initialLeader.nodeId());

            var stableLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.awaitMembershipSize(stableLeader.nodeId(), 3, CLUSTER_TIMEOUT);

            cluster.awaitNodeValue(initialLeader.nodeId(), "alpha", "one", CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(initialLeader.nodeId(), "beta", "two", CLUSTER_TIMEOUT);
        }
    }

    @Test
    void restartedFollowerCatchesUpOnMissedWrites() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String followerId = anyFollower(cluster, leader.nodeId());

            cluster.stopNode(followerId);

            try (var client = cluster.newKvClient()) {
                client.put(Bytes.utf8("gamma"), Bytes.utf8("three")).get();
                client.put(Bytes.utf8("delta"), Bytes.utf8("four")).get();
            }

            cluster.startNode(followerId);
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            cluster.awaitNodeValue(followerId, "gamma", "three", CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(followerId, "delta", "four", CLUSTER_TIMEOUT);
        }
    }

    @Test
    void clusterRecoversCommittedDataAfterFullRestart() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            try (var client = cluster.newKvClient()) {
                client.put(Bytes.utf8("persist-1"), Bytes.utf8("value-1")).get();
                client.put(Bytes.utf8("persist-2"), Bytes.utf8("value-2")).get();
                client.put(Bytes.utf8("persist-3"), Bytes.utf8("value-3")).get();
            }

            for (String nodeId : cluster.runningNodeIds()) {
                cluster.awaitNodeValue(nodeId, "persist-1", "value-1", CLUSTER_TIMEOUT);
                cluster.awaitNodeValue(nodeId, "persist-2", "value-2", CLUSTER_TIMEOUT);
                cluster.awaitNodeValue(nodeId, "persist-3", "value-3", CLUSTER_TIMEOUT);
            }

            cluster.stopAll();
            cluster.startAll();

            var recoveredLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.awaitMembershipSize(recoveredLeader.nodeId(), 3, CLUSTER_TIMEOUT);

            for (String nodeId : cluster.runningNodeIds()) {
                cluster.awaitNodeValue(nodeId, "persist-1", "value-1", CLUSTER_TIMEOUT);
                cluster.awaitNodeValue(nodeId, "persist-2", "value-2", CLUSTER_TIMEOUT);
                cluster.awaitNodeValue(nodeId, "persist-3", "value-3", CLUSTER_TIMEOUT);
            }
        }
    }

    @Test
    void restartedFollowerReplaysLargerBurstOfMissedWrites() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String followerId = anyFollower(cluster, leader.nodeId());

            cluster.stopNode(followerId);

            try (var client = cluster.newKvClient()) {
                for (int i = 0; i < REPLAY_WRITE_COUNT; i++) {
                    client.put(Bytes.utf8("burst-" + i), Bytes.utf8("value-" + i)).get();
                }
            }

            cluster.startNode(followerId);
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.awaitMembershipSize(followerId, 3, CLUSTER_TIMEOUT);

            for (int i = 0; i < REPLAY_WRITE_COUNT; i++) {
                cluster.awaitNodeValue(followerId, "burst-" + i, "value-" + i, CLUSTER_TIMEOUT);
            }
        }
    }

    private TestCluster<InProcessNode> newCluster() throws IOException {
        return TestCluster.create(3, spec -> new InProcessNode(tempDir, spec));
    }

    private static String anyFollower(TestCluster<InProcessNode> cluster, String leaderId) {
        return cluster.runningNodeIds().stream()
            .filter(nodeId -> !nodeId.equals(leaderId))
            .findFirst()
            .orElseThrow();
    }
}
