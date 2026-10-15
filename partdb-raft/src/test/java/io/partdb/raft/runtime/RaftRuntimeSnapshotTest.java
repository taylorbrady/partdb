package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftOptions;
import io.partdb.raft.RaftRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftRuntimeSnapshotTest {

    private static final RaftMembership MEMBERSHIP = RaftMembership.voters("n1", "n2");

    @TempDir
    Path tempDir;

    @Test
    void followerCatchesUpFromStreamedSnapshot() throws Exception {
        Path payload = tempDir.resolve("payload");
        String snapshotContents = "snapshot-state-".repeat(1000);
        Files.writeString(payload, snapshotContents);
        RaftRuntime.bootstrap(tempDir.resolve("n1"), config("n1"), 5, payload);

        var network = new Network();
        var leaderState = new RecordingStateMachine(5);
        var followerState = new RecordingStateMachine(0);

        try (
            var leader = RaftRuntime.open(tempDir.resolve("n1"), config("n1"), network.transport("n1"), leaderState);
            var follower = RaftRuntime.open(tempDir.resolve("n2"), config("n2"), network.transport("n2"), followerState)
        ) {
            await(() -> leader.status().role() == RaftRole.LEADER);
            await(() -> followerState.appliedIndex() >= 5);

            assertEquals(snapshotContents, followerState.restored);
            assertEquals(5, followerState.restoredIndex);

            Bytes result = leader.propose(Bytes.utf8("after-snapshot")).get(5, TimeUnit.SECONDS);
            assertEquals(Bytes.utf8("after-snapshot"), result);
            await(() -> followerState.commands.contains("after-snapshot"));
            assertTrue(follower.status().appliedIndex() > 5);
        }
    }

    private static RaftConfig config(String nodeId) {
        return new RaftConfig(nodeId, MEMBERSHIP, Duration.ofMillis(5), RaftOptions.defaults());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("Condition not met before timeout");
    }

    private static final class RecordingStateMachine implements StateMachine {
        private final List<String> commands = new CopyOnWriteArrayList<>();
        private volatile long applied;
        private volatile String restored;
        private volatile long restoredIndex;

        private RecordingStateMachine(long applied) {
            this.applied = applied;
        }

        @Override
        public long appliedIndex() {
            return applied;
        }

        @Override
        public Bytes apply(long index, Bytes command) {
            commands.add(command.utf8());
            applied = index;
            return command;
        }

        @Override
        public void restore(long index, InputStream snapshot) throws IOException {
            restored = new String(snapshot.readAllBytes(), StandardCharsets.UTF_8);
            restoredIndex = index;
            applied = index;
        }
    }

    private static final class Network {
        private final Map<String, RaftTransport.Inbound> nodes = new ConcurrentHashMap<>();

        RaftTransport transport(String self) {
            return new RaftTransport() {
                @Override
                public void start(Inbound inbound) {
                    nodes.put(self, inbound);
                }

                @Override
                public CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request) {
                    var target = nodes.get(to);
                    if (target == null) {
                        return CompletableFuture.failedFuture(new IllegalStateException("unreachable: " + to));
                    }
                    return target.receive(self, request);
                }

                @Override
                public CompletableFuture<RaftMessage.InstallSnapshotResponse> sendSnapshot(
                    String to,
                    RaftMessage.InstallSnapshot request,
                    Path payload
                ) {
                    var target = nodes.get(to);
                    if (target == null) {
                        return CompletableFuture.failedFuture(new IllegalStateException("unreachable: " + to));
                    }
                    try (FileChannel file = FileChannel.open(payload, StandardOpenOption.READ)) {
                        var receiver = target.receiveSnapshot(self, request);
                        ByteBuffer chunk = ByteBuffer.allocate(1024);
                        while (file.read(chunk.clear()) >= 0) {
                            receiver.write(chunk.flip());
                        }
                        return receiver.complete();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                public void close() {
                    nodes.remove(self);
                }
            };
        }
    }
}
