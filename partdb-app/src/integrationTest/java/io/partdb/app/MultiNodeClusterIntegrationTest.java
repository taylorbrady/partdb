package io.partdb.app;

import io.partdb.app.testing.TestCluster;
import io.partdb.bytes.Bytes;
import io.partdb.client.Condition;
import io.partdb.client.KeyRange;
import io.partdb.client.KeyValue;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiNodeClusterIntegrationTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(10);
    private static final int BURST = 32;
    private static final int THREADS = 8;
    private static final int KEYS_PER_THREAD = 50;
    private static final int OPS_PER_THREAD = 150;
    private static final int COUNTERS = 2;
    private static final int INCREMENTS_PER_THREAD = 25;

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
        try (var cluster = TestCluster.create(3, spec -> new InProcessNode(tempDir, spec, config -> config.logRetentionEntries(8)))) {
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

            var scan = client.kv().scan(KeyRange.between(bytes("proof/"), bytes("proof0")));
            try (var entries = scan.value()) {
                assertEquals(List.of("proof/a=one", "proof/b=two", "proof/c=three"),
                    entries.map(entry -> entry.key().utf8() + "=" + entry.value().utf8()).toList());
            }
            assertTrue(scan.revision() >= last);
        }
    }

    @Test
    void concurrentWritersLeaveEveryNodeWithTheSameData() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var kv = cluster.client().kv();

            var expected = new TreeMap<String, String>();
            for (var written : runConcurrently(thread -> () -> {
                var random = new Random(thread);
                var model = new HashMap<String, String>();
                for (int op = 0; op < OPS_PER_THREAD; op++) {
                    String key = "load/" + thread + "/" + random.nextInt(KEYS_PER_THREAD);
                    if (random.nextInt(10) == 0) {
                        kv.delete(bytes(key));
                        model.remove(key);
                    } else {
                        String value = "value-" + op;
                        kv.put(bytes(key), bytes(value));
                        model.put(key, value);
                    }
                }
                return model;
            })) {
                expected.putAll(written);
            }

            var range = KeyRange.between(bytes("load/"), bytes("load0"));
            try (var entries = kv.scan(range).value()) {
                assertEquals(expected, contents(entries.toList()));
            }
            for (String nodeId : cluster.startedNodeIds()) {
                cluster.awaitCaughtUp(nodeId, CLUSTER_TIMEOUT);
                assertEquals(expected, contents(cluster.localScan(nodeId, range)), nodeId);
            }
        }
    }

    @Test
    void concurrentCompareAndSetIncrementsLoseNoUpdate() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            var kv = cluster.client().kv();
            for (int counter = 0; counter < COUNTERS; counter++) {
                kv.put(bytes("counter-" + counter), bytes("0"));
            }

            var committed = new int[COUNTERS];
            for (var increments : runConcurrently(thread -> () -> {
                var perCounter = new int[COUNTERS];
                for (int i = 0; i < INCREMENTS_PER_THREAD; i++) {
                    int counter = (thread + i) % COUNTERS;
                    var key = bytes("counter-" + counter);
                    TxnResult result;
                    do {
                        var current = kv.get(key).value().orElseThrow();
                        var next = bytes(Integer.toString(Integer.parseInt(current.value().utf8()) + 1));
                        result = kv.transact(Txn.when(List.of(Condition.revisionEquals(key, current.revision())), List.of(Mutation.put(key, next))));
                    } while (result instanceof TxnResult.ConditionFailed);
                    perCounter[counter]++;
                }
                return perCounter;
            })) {
                for (int counter = 0; counter < COUNTERS; counter++) {
                    committed[counter] += increments[counter];
                }
            }

            for (int counter = 0; counter < COUNTERS; counter++) {
                assertEquals(Integer.toString(committed[counter]), value(cluster.client(), "counter-" + counter));
            }
        }
    }

    private static <T> List<T> runConcurrently(IntFunction<Callable<T>> task) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<T>>();
            for (int thread = 0; thread < THREADS; thread++) {
                futures.add(executor.submit(task.apply(thread)));
            }
            var results = new ArrayList<T>();
            for (var future : futures) {
                results.add(future.get());
            }
            return results;
        }
    }

    private static Map<String, String> contents(List<KeyValue> entries) {
        var contents = new TreeMap<String, String>();
        entries.forEach(entry -> contents.put(entry.key().utf8(), entry.value().utf8()));
        return contents;
    }

    private TestCluster<InProcessNode> newCluster() throws IOException {
        return TestCluster.create(3, spec -> new InProcessNode(tempDir, spec));
    }

    private static String anyFollower(TestCluster<InProcessNode> cluster, String leaderId) {
        return cluster.startedNodeIds().stream().filter(nodeId -> !nodeId.equals(leaderId)).findFirst().orElseThrow();
    }
}
