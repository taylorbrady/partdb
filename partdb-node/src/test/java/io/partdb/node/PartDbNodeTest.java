package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.node.cluster.NodeRole;
import io.partdb.node.lease.LeaseGrant;
import io.partdb.node.lease.LeaseId;
import io.partdb.node.replication.ReplicationRpc;
import io.partdb.node.replication.ReplicationTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbNodeTest {

    @TempDir
    Path tempDir;

    @Test
    void linearizableBarrierCompletesOnSingleNode() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config, new NoOpReplicationTransport())) {
            awaitLeader(node);

            node.keyValues().put(bytes("key"), bytes("value")).toCompletableFuture().get(5, TimeUnit.SECONDS);

            long barrierIndex = node.cluster().status().appliedIndex();

            assertTrue(barrierIndex > 0);
            assertEquals(
                bytes("value"),
                node.keyValues().get(bytes("key")).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow().value()
            );
        }
    }

    @Test
    void restartReplaysCommittedLogIntoFreshStateMachineStorage() throws Exception {
        var nodeDirectory = tempDir.resolve("node-1");
        var config = PartDbNodeConfig.builder("node-1", nodeDirectory)
            .tickInterval(Duration.ofMillis(1))
            .build();

        LeaseGrant leaseGrant;
        try (var node = PartDbNode.open(config, new NoOpReplicationTransport())) {
            awaitLeader(node);

            leaseGrant = node.leases().grant(Duration.ofSeconds(30)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            node.keyValues().put(bytes("lease-key"), bytes("lease-value"), leaseGrant.leaseId())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        }

        deleteRecursively(nodeDirectory.resolve("db"));

        try (var recovered = PartDbNode.open(config, new NoOpReplicationTransport())) {
            awaitLeader(recovered);

            assertEquals(
                bytes("lease-value"),
                recovered.keyValues().get(bytes("lease-key")).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow().value()
            );
        }
    }

    @Test
    void putWithMissingLeaseFailsWithoutWritingHiddenValue() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config, new NoOpReplicationTransport())) {
            awaitLeader(node);

            var error = assertThrows(
                ExecutionException.class,
                () -> node.keyValues().put(bytes("key"), bytes("value"), LeaseId.of(42))
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)
            );

            var cause = assertInstanceOf(PartDbException.LeaseNotFound.class, error.getCause());
            assertEquals(42, cause.leaseId());
            assertTrue(node.keyValues().getLocal(bytes("key")).isEmpty());
        }
    }

    @Test
    void keepAliveAndRevokeMissingLeaseFail() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config, new NoOpReplicationTransport())) {
            awaitLeader(node);

            var keepAliveError = assertThrows(
                ExecutionException.class,
                () -> node.leases().keepAlive(LeaseId.of(42)).toCompletableFuture().get(5, TimeUnit.SECONDS)
            );
            assertEquals(42, assertInstanceOf(PartDbException.LeaseNotFound.class, keepAliveError.getCause()).leaseId());

            var revokeError = assertThrows(
                ExecutionException.class,
                () -> node.leases().revoke(LeaseId.of(42)).toCompletableFuture().get(5, TimeUnit.SECONDS)
            );
            assertEquals(42, assertInstanceOf(PartDbException.LeaseNotFound.class, revokeError.getCause()).leaseId());
        }
    }

    @Test
    void revokingOldLeaseDoesNotDeleteOverwrittenUnleasedValue() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config, new NoOpReplicationTransport())) {
            awaitLeader(node);

            LeaseGrant leaseGrant = node.leases().grant(Duration.ofSeconds(30)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            node.keyValues().put(bytes("key"), bytes("lease-value"), leaseGrant.leaseId())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
            node.keyValues().put(bytes("key"), bytes("plain-value"))
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

            node.leases().revoke(leaseGrant.leaseId()).toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertEquals(
                bytes("plain-value"),
                node.keyValues().get(bytes("key")).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow().value()
            );
        }
    }

    private static void awaitLeader(PartDbNode node) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            if (node.cluster().status().role() == NodeRole.LEADER) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("Node did not become leader");
    }

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    private static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }

    private static final class NoOpReplicationTransport implements ReplicationTransport {
        @Override
        public void start(RpcHandler handler) {
        }

        @Override
        public CompletableFuture<ReplicationRpc.Response> send(String to, ReplicationRpc.Request request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("single-node transport"));
        }

        @Override
        public void close() {
        }
    }
}
