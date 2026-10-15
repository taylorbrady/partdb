package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.Condition;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValue;
import io.partdb.node.kv.KvLimits;
import io.partdb.node.kv.Mutation;
import io.partdb.node.kv.ReadConsistency;
import io.partdb.node.kv.Txn;
import io.partdb.node.kv.TxnResult;
import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.Options;
import io.partdb.raft.Role;
import io.partdb.raft.runtime.ClusterId;
import io.partdb.raft.runtime.RaftConfig;
import io.partdb.raft.runtime.SnapshotPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final NodeId SELF = NodeId.of("node-1");

    @TempDir
    Path directory;

    private NodeConfig config(Path data, SnapshotPolicy snapshots, KvLimits limits) {
        var membership = Membership.ofVoters(Set.of(SELF));
        var raft = new RaftConfig(ClusterId.of("test"), SELF, membership, Duration.ofMillis(1), Options.defaults(), snapshots);
        return new NodeConfig(data, raft, limits);
    }

    private NodeConfig config() {
        return config(directory.resolve("node-1"), SnapshotPolicy.DEFAULT, KvLimits.DEFAULT);
    }

    private static Node leader(NodeConfig config) throws InterruptedException {
        var node = Node.standalone(config);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (node.cluster().status().role() != Role.LEADER) {
            if (System.nanoTime() > deadline) {
                node.close();
                throw new AssertionError("node did not become leader");
            }
            Thread.sleep(2);
        }
        return node;
    }

    @Test
    void readsItsOwnWritesWithRevisions() throws Exception {
        try (var node = leader(config())) {
            long revision = node.kv().put(bytes("key"), bytes("value"), TIMEOUT);

            var read = node.kv().get(bytes("key"), ReadConsistency.LINEARIZABLE, TIMEOUT);

            assertEquals(new KeyValue(bytes("key"), bytes("value"), revision), read.value().orElseThrow());
            assertTrue(read.revision() >= revision);
        }
    }

    @Test
    void deleteRemovesTheKey() throws Exception {
        try (var node = leader(config())) {
            long put = node.kv().put(bytes("key"), bytes("value"), TIMEOUT);
            long delete = node.kv().delete(bytes("key"), TIMEOUT);

            assertTrue(delete > put);
            assertTrue(node.kv().get(bytes("key"), ReadConsistency.LOCAL, TIMEOUT).value().isEmpty());
        }
    }

    @Test
    void writeAppliesAllMutationsAtOneRevision() throws Exception {
        try (var node = leader(config())) {
            long revision = node.kv().write(List.of(Mutation.put(bytes("a"), bytes("1")), Mutation.put(bytes("b"), bytes("2"))), TIMEOUT);

            assertEquals(revision, node.kv().get(bytes("a"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().orElseThrow().revision());
            assertEquals(revision, node.kv().get(bytes("b"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().orElseThrow().revision());
        }
    }

    @Test
    void transactionAppliesWhenEveryConditionHolds() throws Exception {
        try (var node = leader(config())) {
            long guard = node.kv().put(bytes("guard"), bytes("expected"), TIMEOUT);

            var result = node.kv().transact(Txn.when(
                List.of(
                    Condition.exists(bytes("guard")),
                    Condition.valueEquals(bytes("guard"), bytes("expected")),
                    Condition.revisionEquals(bytes("guard"), guard),
                    Condition.missing(bytes("missing"))
                ),
                List.of(Mutation.put(bytes("key"), bytes("value")), Mutation.delete(bytes("guard")))
            ), TIMEOUT);

            var committed = assertInstanceOf(TxnResult.Committed.class, result);
            assertTrue(committed.revision() > guard);
            assertEquals(bytes("value"), node.kv().get(bytes("key"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().orElseThrow().value());
            assertTrue(node.kv().get(bytes("guard"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().isEmpty());
        }
    }

    @Test
    void transactionWritesNothingWhenAConditionFails() throws Exception {
        try (var node = leader(config())) {
            node.kv().put(bytes("guard"), bytes("actual"), TIMEOUT);

            var result = node.kv().transact(Txn.when(
                List.of(Condition.valueEquals(bytes("guard"), bytes("expected"))),
                List.of(Mutation.put(bytes("key"), bytes("value")))
            ), TIMEOUT);

            assertInstanceOf(TxnResult.ConditionFailed.class, result);
            assertTrue(node.kv().get(bytes("key"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().isEmpty());
        }
    }

    @Test
    void scanReturnsAConsistentRangeWithItsReadRevision() throws Exception {
        try (var node = leader(config())) {
            for (String key : List.of("a", "b", "c", "d")) {
                node.kv().put(bytes(key), bytes(key + "-value"), TIMEOUT);
            }

            var keys = new ArrayList<String>();
            long revision;
            try (var scan = node.kv().scan(KeyRange.between(bytes("b"), bytes("d")), 0, ReadConsistency.LINEARIZABLE, TIMEOUT)) {
                revision = scan.readRevision();
                scan.forEachRemaining(entry -> keys.add(entry.key().utf8()));
            }

            assertEquals(List.of("b", "c"), keys);
            assertTrue(revision > 0);
        }
    }

    @Test
    void scanHonoursItsLimit() throws Exception {
        try (var node = leader(config())) {
            for (String key : List.of("a", "b", "c")) {
                node.kv().put(bytes(key), bytes("v"), TIMEOUT);
            }

            try (var scan = node.kv().scan(KeyRange.all(), 2, ReadConsistency.LINEARIZABLE, TIMEOUT)) {
                var keys = new ArrayList<String>();
                scan.forEachRemaining(entry -> keys.add(entry.key().utf8()));
                assertEquals(List.of("a", "b"), keys);
            }
        }
    }

    @Test
    void rejectsRequestsOutsideTheLimitsBeforeProposing() throws Exception {
        var limits = new KvLimits(4, 8, 2, 2, 64);
        try (var node = leader(config(directory.resolve("node-1"), SnapshotPolicy.DEFAULT, limits))) {
            assertThrows(NodeException.InvalidRequest.class, () -> node.kv().put(bytes("too-long"), bytes("v"), TIMEOUT));
            assertThrows(NodeException.InvalidRequest.class, () -> node.kv().put(bytes("k"), bytes("value-too-long"), TIMEOUT));
            assertThrows(NodeException.InvalidRequest.class, () -> node.kv().put(Bytes.EMPTY, bytes("v"), TIMEOUT));
            assertThrows(NodeException.InvalidRequest.class, () -> node.kv().write(List.of(
                Mutation.put(bytes("a"), bytes("1")), Mutation.put(bytes("b"), bytes("2")), Mutation.put(bytes("c"), bytes("3"))), TIMEOUT));
            assertEquals(0, node.metrics().proposals());
        }
    }

    @Test
    void restartReplaysTheLogIntoFreshStorage() throws Exception {
        var config = config();
        try (var node = leader(config)) {
            node.kv().put(bytes("key"), bytes("value"), TIMEOUT);
        }
        deleteRecursively(config.dataDirectory().resolve("db"));

        try (var recovered = leader(config)) {
            assertEquals(bytes("value"), recovered.kv().get(bytes("key"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().orElseThrow().value());
        }
    }

    @Test
    void restartRestoresFromASnapshotAndReplaysTheTail() throws Exception {
        var config = config(directory.resolve("node-1"), SnapshotPolicy.everyEntries(5), KvLimits.DEFAULT);
        try (var node = leader(config)) {
            for (int i = 0; i < 23; i++) {
                node.kv().put(bytes("k" + i), bytes("v" + i), TIMEOUT);
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (node.cluster().status().snapshotIndex() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(2);
            }
            assertTrue(node.cluster().status().snapshotIndex() > 0);
        }

        try (var recovered = leader(config)) {
            for (int i = 0; i < 23; i++) {
                assertEquals(bytes("v" + i), recovered.kv().get(bytes("k" + i), ReadConsistency.LINEARIZABLE, TIMEOUT).value().orElseThrow().value());
            }
        }
    }

    @Test
    void snapshotsAdvancePastEntriesThatChangeNothing() throws Exception {
        var config = config(directory.resolve("node-1"), SnapshotPolicy.everyEntries(5), KvLimits.DEFAULT);
        try (var node = leader(config)) {
            long write = node.kv().put(bytes("guard"), bytes("actual"), TIMEOUT);
            for (int i = 0; i < 20; i++) {
                var result = node.kv().transact(Txn.when(
                    List.of(Condition.valueEquals(bytes("guard"), bytes("expected"))),
                    List.of(Mutation.put(bytes("never"), bytes("written")))
                ), TIMEOUT);
                assertInstanceOf(TxnResult.ConditionFailed.class, result);
            }
            long applied = node.cluster().status().appliedIndex();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (node.cluster().status().snapshotIndex() <= write + 5 && System.nanoTime() < deadline) {
                Thread.sleep(2);
            }

            assertTrue(node.cluster().status().snapshotIndex() > write + 5,
                "snapshot index " + node.cluster().status().snapshotIndex() + " stayed near the last real write " + write);
            assertTrue(applied >= write + 20);
        }

        try (var recovered = leader(config)) {
            assertEquals(bytes("actual"), recovered.kv().get(bytes("guard"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().orElseThrow().value());
            assertTrue(recovered.kv().get(bytes("never"), ReadConsistency.LINEARIZABLE, TIMEOUT).value().isEmpty());
        }
    }

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
