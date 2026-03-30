package io.partdb.node;

import io.partdb.node.transport.ConsensusMessage;
import io.partdb.node.transport.ConsensusTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbNodeTest {

    @TempDir
    Path tempDir;

    @Test
    void linearizableBarrierCompletesOnSingleNode() throws Exception {
        var config = PartDbNodeConfig.builder("node-1", tempDir.resolve("node-1"))
            .tickInterval(Duration.ofMillis(1))
            .build();

        try (var node = new PartDbNode(config, new NoOpConsensusTransport())) {
            awaitLeader(node);

            node.put(bytes("key"), bytes("value"), 0).get(5, TimeUnit.SECONDS);

            long barrierIndex = node.linearizableBarrier().get(5, TimeUnit.SECONDS);

            assertTrue(barrierIndex > 0);
            assertArrayEquals(bytes("value"), node.get(bytes("key")).orElseThrow());
        }
    }

    private static void awaitLeader(PartDbNode node) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            if (node.status().role() == NodeRole.LEADER) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("Node did not become leader");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class NoOpConsensusTransport implements ConsensusTransport {
        @Override
        public void start(RpcHandler handler) {
        }

        @Override
        public CompletableFuture<ConsensusMessage.Response> send(String to, ConsensusMessage.Request request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("single-node transport"));
        }

        @Override
        public void close() {
        }
    }
}
