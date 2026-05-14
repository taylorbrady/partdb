package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.node.cluster.NodeRole;
import io.partdb.node.kv.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbNodeTest {

    @TempDir
    Path tempDir;

    @Test
    void readBarrierCompletesOnSingleNode() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config)) {
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

        try (var node = PartDbNode.open(config)) {
            awaitLeader(node);

            node.keyValues().put(bytes("key"), bytes("value"))
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        }

        deleteRecursively(nodeDirectory.resolve("db"));

        try (var recovered = PartDbNode.open(config)) {
            awaitLeader(recovered);

            assertEquals(
                bytes("value"),
                recovered.keyValues().get(bytes("key")).toCompletableFuture().get(5, TimeUnit.SECONDS)
                    .orElseThrow()
                    .value()
            );
        }
    }

    @Test
    void writeBatchAppliesAllWritesAtSingleRevision() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = PartDbNode.open(config)) {
            awaitLeader(node);

            long revision = node.keyValues()
                .writeBatch(WriteBatch.builder()
                    .put(bytes("key-1"), bytes("value-1"))
                    .put(bytes("key-2"), bytes("value-2"))
                    .build())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS)
                .modRevision();

            assertEquals(revision, node.keyValues().get(bytes("key-1")).toCompletableFuture().get(5, TimeUnit.SECONDS)
                .orElseThrow()
                .modRevision());
            assertEquals(revision, node.keyValues().get(bytes("key-2")).toCompletableFuture().get(5, TimeUnit.SECONDS)
                .orElseThrow()
                .modRevision());
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
}
