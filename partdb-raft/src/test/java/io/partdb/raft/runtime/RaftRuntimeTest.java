package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.MembershipChange;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftOptions;
import io.partdb.raft.RaftRole;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftRuntimeTest {

    private static final RaftMembership THREE = RaftMembership.voters("n1", "n2", "n3");

    @TempDir
    Path directory;

    private final LocalNetwork network = new LocalNetwork();
    private final Map<String, RaftRuntime> runtimes = new LinkedHashMap<>();
    private final Map<String, RecordingStateMachine> machines = new LinkedHashMap<>();

    @AfterEach
    void closeAll() {
        runtimes.values().forEach(RaftRuntime::close);
    }

    private long snapshotThreshold = RaftConfig.DEFAULT_SNAPSHOT_THRESHOLD;

    private RaftConfig config(String id, RaftMembership membership) {
        return new RaftConfig(id, membership, Duration.ofMillis(5), RaftOptions.defaults(), snapshotThreshold);
    }

    private RaftRuntime start(String id, RaftMembership membership) {
        var machine = machines.computeIfAbsent(id, _ -> new RecordingStateMachine());
        var runtime = RaftRuntime.open(directory.resolve(id), config(id, membership), network.transport(id), machine);
        runtimes.put(id, runtime);
        return runtime;
    }

    private void restart(String id, RaftMembership membership) {
        runtimes.remove(id).close();
        machines.remove(id);
        start(id, membership);
    }

    private void startAll(RaftMembership membership) {
        membership.members().forEach(id -> start(id, membership));
    }

    private RaftRuntime leader() {
        await(() -> runtimes.values().stream().anyMatch(runtime -> runtime.status().role() == RaftRole.LEADER));
        return runtimes.values().stream().filter(runtime -> runtime.status().role() == RaftRole.LEADER).findFirst().orElseThrow();
    }

    private RaftRuntime follower() {
        var leader = leader();
        return runtimes.values().stream().filter(runtime -> runtime != leader).findFirst().orElseThrow();
    }

    private static <T> T join(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static Throwable failure(CompletionStage<?> stage) {
        var error = assertThrows(ExecutionException.class, () -> join(stage)).getCause();
        return error instanceof CompletionException completion ? completion.getCause() : error;
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met in time");
            }
            Thread.onSpinWait();
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    @Test
    void replicatesConcurrentProposalsInOneOrder() throws Exception {
        startAll(THREE);
        var leader = leader();

        var results = new ArrayList<CompletionStage<Bytes>>();
        for (int i = 0; i < 200; i++) {
            results.add(leader.propose(Bytes.utf8("c" + i)));
        }
        for (int i = 0; i < results.size(); i++) {
            assertEquals(Bytes.utf8("c" + i), join(results.get(i)));
        }

        await(() -> machines.values().stream().allMatch(machine -> machine.commands.size() == 200));
        var expected = machines.get(leader.status().nodeId()).commands;
        machines.values().forEach(machine -> assertEquals(expected, machine.commands));
    }

    @Test
    void followerRejectsProposalsWithLeaderHint() throws Exception {
        startAll(THREE);
        var leader = leader();
        await(() -> follower().status().leaderId().isPresent());

        var error = assertInstanceOf(RaftException.NotLeader.class, failure(follower().propose(Bytes.utf8("x"))));

        assertEquals(leader.status().nodeId(), error.leaderId().orElseThrow());
    }

    @Test
    void followerReadBarrierWaitsForAppliedState() throws Exception {
        startAll(THREE);
        join(leader().propose(Bytes.utf8("a")));
        var follower = follower();
        await(() -> follower.status().leaderId().isPresent());

        long index = join(follower.readBarrier());

        var machine = machines.get(follower.status().nodeId());
        assertTrue(machine.applied >= index);
        assertTrue(machine.commands.contains("a"));
    }

    @Test
    void leaderlessReadBarrierFailsPromptly() {
        var runtime = start("n1", THREE);

        assertInstanceOf(RaftException.NotLeader.class, failure(runtime.readBarrier()));
    }

    @Test
    void closedRuntimeRejectsRequests() {
        var runtime = start("n1", RaftMembership.voters("n1"));
        runtime.close();
        runtimes.remove("n1");

        assertInstanceOf(RaftException.Closed.class, failure(runtime.propose(Bytes.utf8("x"))));
        assertTrue(!runtime.status().running());
    }

    @Test
    void restartReplaysCommittedEntriesIntoFreshStateMachine() throws Exception {
        var single = RaftMembership.voters("n1");
        var runtime = start("n1", single);
        await(() -> runtime.status().role() == RaftRole.LEADER);
        for (int i = 0; i < 5; i++) {
            join(runtime.propose(Bytes.utf8("c" + i)));
        }
        runtime.close();
        runtimes.remove("n1");
        machines.remove("n1");

        var restarted = start("n1", single);

        assertEquals(List.of("c0", "c1", "c2", "c3", "c4"), machines.get("n1").commands);
        await(() -> restarted.status().role() == RaftRole.LEADER);
        assertEquals(Bytes.utf8("next"), join(restarted.propose(Bytes.utf8("next"))));
    }

    @Test
    void transfersLeadershipToAFollower() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));
        String target = follower().status().nodeId();

        join(leader.transferLeadership(target));

        await(() -> runtimes.get(target).status().role() == RaftRole.LEADER);
        assertEquals(Bytes.utf8("b"), join(runtimes.get(target).propose(Bytes.utf8("b"))));
    }

    @Test
    void addsAndPromotesANewMember() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));
        start("n4", RaftMembership.of(Set.of("n1", "n2", "n3"), Set.of("n4")));

        var learner = join(leader.changeMembership(new MembershipChange.AddLearner("n4")));
        assertTrue(learner.isLearner("n4"));
        var promoted = join(leader.changeMembership(new MembershipChange.PromoteToVoter("n4")));
        assertTrue(promoted.isVoter("n4"));

        join(leader.propose(Bytes.utf8("b")));
        await(() -> machines.get("n4").commands.equals(List.of("a", "b")));
        await(() -> runtimes.get("n4").membership().isVoter("n4"));
    }

    @Test
    void laggingFollowerCatchesUpFromStreamedSnapshot() throws Exception {
        var two = RaftMembership.voters("n1", "n2");
        Path payload = directory.resolve("payload");
        String contents = "snapshot-state-".repeat(1000);
        Files.writeString(payload, contents);
        RaftRuntime.bootstrap(directory.resolve("n1"), config("n1", two), 5, payload);
        machines.put("n1", new RecordingStateMachine(5));

        start("n1", two);
        start("n2", two);
        var leader = leader();
        assertEquals("n1", leader.status().nodeId());

        await(() -> machines.get("n2").applied >= 5);
        assertEquals(contents, machines.get("n2").restored);
        join(leader.propose(Bytes.utf8("after")));
        await(() -> machines.get("n2").commands.contains("after"));
    }

    @Test
    void isolatedLeaderStepsDownAndClusterMovesOn() throws Exception {
        startAll(THREE);
        var oldLeader = leader();
        String oldId = oldLeader.status().nodeId();

        network.isolate(oldId);
        await(() -> oldLeader.status().role() != RaftRole.LEADER);
        await(() -> runtimes.values().stream()
            .anyMatch(runtime -> runtime != oldLeader && runtime.status().role() == RaftRole.LEADER));
        var newLeader = runtimes.values().stream()
            .filter(runtime -> runtime != oldLeader && runtime.status().role() == RaftRole.LEADER)
            .findFirst().orElseThrow();
        join(newLeader.propose(Bytes.utf8("during-partition")));

        network.heal(oldId);
        await(() -> machines.get(oldId).commands.contains("during-partition"));
    }

    @Test
    void compactsTheLogOnceTheThresholdIsReached() throws Exception {
        snapshotThreshold = 20;
        startAll(THREE);
        var leader = leader();

        for (int i = 0; i < 100; i++) {
            join(leader.propose(Bytes.utf8("c" + i)));
        }

        await(() -> runtimes.values().stream().allMatch(runtime -> runtime.status().snapshotIndex() >= 80));
        var expected = machines.get(leader.status().nodeId()).commands;
        await(() -> machines.values().stream().allMatch(machine -> machine.commands.equals(expected)));
    }

    @Test
    void followerBehindCompactionCatchesUpFromLeaderSnapshot() throws Exception {
        snapshotThreshold = 10;
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("first")));
        String lagging = follower().status().nodeId();
        network.isolate(lagging);

        for (int i = 0; i < 50; i++) {
            join(leader.propose(Bytes.utf8("c" + i)));
        }
        await(() -> leader.status().snapshotIndex() > 20);
        network.heal(lagging);

        var expected = machines.get(leader.status().nodeId()).commands;
        await(() -> machines.get(lagging).commands.equals(expected));
        assertTrue(machines.get(lagging).restored != null);
    }

    @Test
    void restartRestoresFromSnapshotAndReplaysTheTail() throws Exception {
        snapshotThreshold = 5;
        var single = RaftMembership.voters("n1");
        var runtime = start("n1", single);
        await(() -> runtime.status().role() == RaftRole.LEADER);
        var expected = new ArrayList<String>();
        for (int i = 0; i < 23; i++) {
            join(runtime.propose(Bytes.utf8("c" + i)));
            expected.add("c" + i);
        }
        await(() -> runtime.status().snapshotIndex() > 0);

        restart("n1", single);

        assertEquals(expected, machines.get("n1").commands);
        assertTrue(machines.get("n1").restored != null);
    }

    private static final class RecordingStateMachine implements StateMachine {
        private final List<String> commands = new CopyOnWriteArrayList<>();
        private volatile long applied;
        private volatile String restored;

        private RecordingStateMachine() {
        }

        private RecordingStateMachine(long applied) {
            this.applied = applied;
        }

        @Override
        public long appliedIndex() {
            return applied;
        }

        @Override
        public synchronized Bytes apply(long index, Bytes command) {
            commands.add(command.utf8());
            applied = index;
            return command;
        }

        @Override
        public synchronized long snapshot(OutputStream target) throws IOException {
            target.write(String.join("\n", commands).getBytes(StandardCharsets.UTF_8));
            return applied;
        }

        @Override
        public synchronized void restore(long index, InputStream snapshot) throws IOException {
            restored = new String(snapshot.readAllBytes(), StandardCharsets.UTF_8);
            commands.clear();
            if (!restored.isEmpty()) {
                commands.addAll(List.of(restored.split("\n")));
            }
            applied = index;
        }
    }
}
