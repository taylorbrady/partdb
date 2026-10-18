package io.partdb.app;

import io.partdb.app.testing.TestCluster;
import io.partdb.bytes.Bytes;
import io.partdb.client.Condition;
import io.partdb.client.KeyRange;
import io.partdb.client.Mutation;
import io.partdb.client.PartDbClient;
import io.partdb.client.ReadConsistency;
import io.partdb.client.Txn;
import io.partdb.client.TxnResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiNodeClusterIntegrationTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(10);
    private static final int BURST = 32;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    private static String value(PartDbClient client, String key) {
        return client.kv().get(bytes(key)).value().orElseThrow().value().utf8();
    }

    @Test
    void conditionalTransactionsCompareAndSetAgainstTheCluster() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var kv = cluster.client().kv();

            var created = kv.transact(Txn.when(List.of(Condition.missing(bytes("config"))), List.of(Mutation.put(bytes("config"), bytes("v1")))));
            long revision = assertInstanceOf(TxnResult.Committed.class, created).revision();
            assertEquals(new TxnResult.ConditionFailed(),
                kv.transact(Txn.when(List.of(Condition.missing(bytes("config"))), List.of(Mutation.put(bytes("config"), bytes("lost"))))));

            var read = kv.get(bytes("config"));
            assertEquals(revision, read.value().orElseThrow().revision());
            assertTrue(read.revision() >= revision);

            var swapped = kv.transact(Txn.when(
                List.of(Condition.revisionEquals(bytes("config"), revision)),
                List.of(Mutation.put(bytes("config"), bytes("v2")), Mutation.put(bytes("audit"), bytes("v1->v2")))));
            long next = assertInstanceOf(TxnResult.Committed.class, swapped).revision();
            assertTrue(next > revision);
            assertEquals(new TxnResult.ConditionFailed(),
                kv.transact(Txn.when(List.of(Condition.revisionEquals(bytes("config"), revision)), List.of(Mutation.delete(bytes("config"))))));
            assertEquals("v2", value(cluster.client(), "config"));
            assertEquals(next, kv.get(bytes("audit")).value().orElseThrow().revision());
        }
    }

    @Test
    void writesSentToAFollowerFirstAreRedirectedToTheLeader() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String follower = anyFollower(cluster, leader.nodeId());

            var endpoints = new ArrayList<String>();
            endpoints.add(cluster.endpoint(follower));
            cluster.startedNodeIds().stream().filter(id -> !id.equals(follower)).map(cluster::endpoint).forEach(endpoints::add);
            try (var client = PartDbClient.connect(endpoints.toArray(String[]::new))) {
                long first = client.kv().put(bytes("a"), bytes("one"));
                long second = client.kv().write(List.of(Mutation.put(bytes("b"), bytes("two")), Mutation.put(bytes("c"), bytes("three"))));

                assertTrue(second > first);
                var entry = client.kv().get(bytes("b")).value().orElseThrow();
                assertEquals(second, entry.revision());
                assertEquals("three", value(client, "c"));
            }
        }
    }

    @Test
    void oneClientRidesThroughLeaderFailover() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var client = cluster.client();
            client.kv().put(bytes("alpha"), bytes("one"));

            cluster.stopNode(initialLeader.nodeId());
            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            assertNotEquals(initialLeader.nodeId(), newLeader.nodeId());

            client.kv().put(bytes("beta"), bytes("two"));
            assertEquals("one", value(client, "alpha"));
            assertEquals("two", value(client, "beta"));

            cluster.startNode(initialLeader.nodeId());
            cluster.awaitCaughtUp(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            cluster.awaitNodeValue(initialLeader.nodeId(), "beta", "two", CLUSTER_TIMEOUT);
        }
    }

    @Test
    void restartedFollowerCatchesUpOnMissedWrites() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String follower = anyFollower(cluster, leader.nodeId());
            cluster.stopNode(follower);

            for (int i = 0; i < BURST; i++) {
                cluster.client().kv().put(bytes("burst-" + i), bytes("value-" + i));
            }

            cluster.startNode(follower);
            cluster.awaitCaughtUp(follower, CLUSTER_TIMEOUT);
            for (int i = 0; i < BURST; i++) {
                assertEquals("value-" + i, cluster.client().kv().get(bytes("burst-" + i), ReadConsistency.LINEARIZABLE).value().orElseThrow().value().utf8());
            }
            cluster.awaitNodeValue(follower, "burst-" + (BURST - 1), "value-" + (BURST - 1), CLUSTER_TIMEOUT);
        }
    }

    @Test
    void clusterRecoversCommittedDataAfterFullRestart() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            for (int i = 1; i <= 3; i++) {
                cluster.client().kv().put(bytes("persist-" + i), bytes("value-" + i));
            }

            cluster.stopAll();
            cluster.startAll();
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            for (String nodeId : cluster.startedNodeIds()) {
                cluster.awaitCaughtUp(nodeId, CLUSTER_TIMEOUT);
                cluster.awaitNodeValue(nodeId, "persist-3", "value-3", CLUSTER_TIMEOUT);
            }
            assertEquals("value-1", value(cluster.client(), "persist-1"));
        }
    }

    @Test
    void restartedFollowerInstallsSnapshotOverThePeerTransport() throws Exception {
        try (var cluster = TestCluster.create(3, spec -> new InProcessNode(tempDir, spec, config -> config.snapshotLogEntries(8)))) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String follower = anyFollower(cluster, leader.nodeId());
            cluster.stopNode(follower);

            for (int i = 0; i < BURST; i++) {
                cluster.client().kv().put(bytes("compacted-" + i), bytes("value-" + i));
            }

            cluster.startNode(follower);
            cluster.awaitCaughtUp(follower, CLUSTER_TIMEOUT);
            assertTrue(cluster.status(follower).snapshotIndex() > 0);
            for (int i = 0; i < BURST; i++) {
                cluster.awaitNodeValue(follower, "compacted-" + i, "value-" + i, CLUSTER_TIMEOUT);
            }
        }
    }

    @Test
    void scanReadsAConsistentRangeInKeyOrder() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var client = cluster.client();
            client.kv().put(bytes("proof/b"), bytes("two"));
            client.kv().put(bytes("proof/a"), bytes("one"));
            long last = client.kv().put(bytes("proof/c"), bytes("three"));
            client.kv().put(bytes("other"), bytes("x"));

            var scanned = new ArrayList<String>();
            long readRevision;
            try (var cursor = client.kv().scan(KeyRange.between(bytes("proof/"), bytes("proof0")))) {
                readRevision = cursor.readRevision();
                cursor.forEachRemaining(entry -> scanned.add(entry.key().utf8() + "=" + entry.value().utf8()));
            }

            assertEquals(List.of("proof/a=one", "proof/b=two", "proof/c=three"), scanned);
            assertTrue(readRevision >= last);
        }
    }

    private TestCluster<InProcessNode> newCluster() throws IOException {
        return TestCluster.create(3, spec -> new InProcessNode(tempDir, spec));
    }

    private static String anyFollower(TestCluster<InProcessNode> cluster, String leaderId) {
        return cluster.startedNodeIds().stream().filter(nodeId -> !nodeId.equals(leaderId)).findFirst().orElseThrow();
    }
}
