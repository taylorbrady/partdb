package io.partdb.app;

import io.partdb.app.testing.TestCluster;
import io.partdb.bytes.Bytes;
import io.partdb.client.KeyRange;
import io.partdb.client.KeyValue;
import io.partdb.client.PartDbException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedCrashRecoveryTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(30);
    private static final KeyRange ALL = KeyRange.all();
    private static final int BURST = 64;
    private static final int WRITERS = 4;
    private static final int WRITES_PER_PHASE = 200;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Test
    void killedFollowerCatchesUpFromASnapshot() throws Exception {
        var app = PackagedApp.fromInstallDir();

        try (var cluster = TestCluster.create(3, spec -> app.newNode(tempDir, spec, "--log-retention-entries", "8"))) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String follower = cluster.startedNodeIds().stream().filter(id -> !id.equals(leader.nodeId())).findFirst().orElseThrow();
            var kv = cluster.client().kv();

            for (int i = 0; i < BURST; i++) {
                kv.put(Bytes.utf8("before-" + i), Bytes.utf8("value-" + i));
            }
            cluster.awaitCaughtUp(follower, CLUSTER_TIMEOUT);
            cluster.node(follower).kill();

            for (int i = 0; i < BURST; i++) {
                kv.put(Bytes.utf8("after-" + i), Bytes.utf8("value-" + i));
            }
            cluster.startNode(follower);
            cluster.awaitCaughtUp(follower, CLUSTER_TIMEOUT);

            String log = cluster.node(follower).log();
            assertTrue(log.contains("\"msg\":\"Installed snapshot\""), follower + " caught up without a snapshot:\n" + log);
            assertEquals(2 * BURST, cluster.localScan(follower, ALL).size());
            assertEquals(scan(cluster), cluster.localScan(follower, ALL));
        }
    }

    @Test
    void killedLeaderLosesNoAcknowledgedWrite() throws Exception {
        var app = PackagedApp.fromInstallDir();

        try (var cluster = TestCluster.create(3, spec -> app.newNode(tempDir, spec))) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var kv = cluster.client().kv();
            var acknowledged = new ConcurrentHashMap<String, String>();
            var stop = new AtomicBoolean();

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var writers = new ArrayList<Future<?>>();
                for (int writer = 0; writer < WRITERS; writer++) {
                    String prefix = "writer-" + writer + "-";
                    writers.add(executor.submit(() -> {
                        for (int i = 0; !stop.get(); i++) {
                            String key = prefix + i;
                            String value = "value-" + i;
                            try {
                                kv.put(Bytes.utf8(key), Bytes.utf8(value));
                                acknowledged.put(key, value);
                            } catch (PartDbException _) {
                            }
                        }
                    }));
                }

                awaitAcknowledged(acknowledged, WRITES_PER_PHASE);
                cluster.node(leader.nodeId()).kill();
                int atKill = acknowledged.size();
                cluster.awaitStableLeaderExcluding(leader.nodeId(), CLUSTER_TIMEOUT);
                awaitAcknowledged(acknowledged, atKill + WRITES_PER_PHASE);
                stop.set(true);
                for (var writer : writers) {
                    writer.get();
                }
            }

            cluster.startNode(leader.nodeId());
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var expected = scan(cluster);
            Map<String, String> stored = expected.stream().collect(Collectors.toMap(entry -> entry.key().utf8(), entry -> entry.value().utf8()));
            acknowledged.forEach((key, value) -> assertEquals(value, stored.get(key), "acknowledged write to " + key));
            for (String nodeId : cluster.startedNodeIds()) {
                cluster.awaitCaughtUp(nodeId, CLUSTER_TIMEOUT);
                assertEquals(expected, cluster.localScan(nodeId, ALL), nodeId + " diverged from the cluster");
            }
        }
    }

    private static List<KeyValue> scan(TestCluster<PackagedNode> cluster) {
        try (var entries = cluster.client().kv().scan(ALL).value()) {
            return entries.toList();
        }
    }

    private static void awaitAcknowledged(Map<String, String> acknowledged, int count) throws InterruptedException {
        long deadline = System.nanoTime() + CLUSTER_TIMEOUT.toNanos();
        while (acknowledged.size() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + count + " acknowledged writes, saw " + acknowledged.size());
            }
            Thread.sleep(10);
        }
    }
}
