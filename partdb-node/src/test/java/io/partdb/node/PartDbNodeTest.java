package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.transport.ConsensusRpc;
import io.partdb.consensus.transport.ConsensusTransport;
import io.partdb.node.cluster.NodeRole;
import io.partdb.node.lease.LeaseGrant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbNodeTest {

    @TempDir
    Path tempDir;

    @Test
    void linearizableBarrierCompletesOnSingleNode() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config, new NoOpConsensusTransport())) {
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
        try (var node = PartDbNode.open(config, new NoOpConsensusTransport())) {
            awaitLeader(node);

            leaseGrant = node.leases().grant(Duration.ofSeconds(30)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            node.keyValues().put(bytes("lease-key"), bytes("lease-value"), leaseGrant.leaseId())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        }

        deleteRecursively(nodeDirectory.resolve("db"));

        try (var recovered = PartDbNode.open(config, new NoOpConsensusTransport())) {
            awaitLeader(recovered);

            assertEquals(
                bytes("lease-value"),
                recovered.keyValues().get(bytes("lease-key")).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow().value()
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

    private static final class NoOpConsensusTransport implements ConsensusTransport {
        @Override
        public void start(RpcHandler handler) {
        }

        @Override
        public CompletableFuture<ConsensusRpc.Response> send(String to, ConsensusRpc.Request request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("single-node transport"));
        }

        @Override
        public void close() {
        }
    }
}
