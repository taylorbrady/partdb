package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Ids;
import io.partdb.raft.Membership;
import io.partdb.raft.MembershipChange;
import io.partdb.raft.NodeId;
import io.partdb.raft.Options;
import io.partdb.raft.Role;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftRuntimeTest {

    private static final Membership THREE = Ids.voters("n1", "n2", "n3");

    @TempDir
    Path directory;

    private final LocalNetwork network = new LocalNetwork();
    private final Map<String, RaftRuntime<Bytes>> runtimes = new LinkedHashMap<>();
    private final Map<String, RecordingStateMachine> machines = new LinkedHashMap<>();
    private SnapshotPolicy snapshots = SnapshotPolicy.DEFAULT;

    @AfterEach
    void closeAll() {
        runtimes.values().forEach(RaftRuntime::close);
    }

    private RaftConfig config(String id, Membership membership) {
        return new RaftConfig(ClusterId.of("test"), Ids.id(id), membership, Duration.ofMillis(5), Options.defaults(), snapshots);
    }

    private RaftRuntime<Bytes> start(String id, Membership membership) {
        var machine = machines.computeIfAbsent(id, _ -> new RecordingStateMachine());
        var runtime = RaftRuntime.open(directory.resolve(id), config(id, membership), network.transport(Ids.id(id)), machine);
        runtimes.put(id, runtime);
        return runtime;
    }

    private void restart(String id, Membership membership) {
        runtimes.remove(id).close();
        machines.remove(id);
        start(id, membership);
    }

    private void startAll(Membership membership) {
        membership.members().forEach(id -> start(id.value(), membership));
    }

    private static String name(RaftRuntime<?> runtime) {
        return runtime.status().nodeId().value();
    }

    private RaftRuntime<Bytes> leader() {
        await(() -> runtimes.values().stream().anyMatch(runtime -> runtime.status().role() == Role.LEADER));
        return runtimes.values().stream().filter(runtime -> runtime.status().role() == Role.LEADER).findFirst().orElseThrow();
    }

    private RaftRuntime<Bytes> follower() {
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
        var expected = machines.get(name(leader)).commands;
        machines.values().forEach(machine -> assertEquals(expected, machine.commands));
    }

    @Test
    void followerRejectsProposalsWithLeaderHint() {
        startAll(THREE);
        var leader = leader();
        await(() -> follower().status().leader().isPresent());

        var error = assertInstanceOf(RaftException.NotLeader.class, failure(follower().propose(Bytes.utf8("x"))));

        assertEquals(leader.status().nodeId(), error.leader().orElseThrow());
    }

    @Test
    void stateMachineSeesEveryIndexIncludingNoOps() throws Exception {
        var runtime = start("n1", Ids.voters("n1"));
        await(() -> runtime.status().role() == Role.LEADER);

        join(runtime.propose(Bytes.utf8("a")));

        var machine = machines.get("n1");
        assertEquals(List.of(1L), machine.skipped);
        assertEquals(2, machine.applied);
    }

    @Test
    void followerReadBarrierWaitsForAppliedState() throws Exception {
        startAll(THREE);
        join(leader().propose(Bytes.utf8("a")));
        var follower = follower();
        await(() -> follower.status().leader().isPresent());

        long index = join(follower.readBarrier());

        var machine = machines.get(name(follower));
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
        var runtime = start("n1", Ids.voters("n1"));
        runtime.close();
        runtimes.remove("n1");

        assertInstanceOf(RaftException.Closed.class, failure(runtime.propose(Bytes.utf8("x"))));
        assertFalse(runtime.status().running());
    }

    @Test
    void refusesADirectoryFromAnotherCluster() {
        start("n1", Ids.voters("n1"));
        runtimes.remove("n1").close();

        var other = new RaftConfig(ClusterId.of("other"), Ids.id("n1"), Ids.voters("n1"), Duration.ofMillis(5), Options.defaults(), snapshots);

        assertThrows(IllegalStateException.class,
            () -> RaftRuntime.open(directory.resolve("n1"), other, network.transport(Ids.id("n1")), new RecordingStateMachine()));
    }

    @Test
    void keepsTheRecordedInitialMembershipAcrossRestarts() throws Exception {
        var runtime = start("n1", Ids.voters("n1"));
        await(() -> runtime.status().role() == Role.LEADER);
        runtimes.remove("n1").close();
        machines.remove("n1");

        var restarted = start("n1", Ids.voters("n1", "n2", "n3"));

        assertEquals(Ids.voters("n1"), restarted.membership());
        await(() -> restarted.status().role() == Role.LEADER);
    }

    @Test
    void restartReplaysCommittedEntriesIntoFreshStateMachine() throws Exception {
        var single = Ids.voters("n1");
        var runtime = start("n1", single);
        await(() -> runtime.status().role() == Role.LEADER);
        for (int i = 0; i < 5; i++) {
            join(runtime.propose(Bytes.utf8("c" + i)));
        }
        runtime.close();
        runtimes.remove("n1");
        machines.remove("n1");

        var restarted = start("n1", single);

        assertEquals(List.of("c0", "c1", "c2", "c3", "c4"), machines.get("n1").commands);
        await(() -> restarted.status().role() == Role.LEADER);
        assertEquals(Bytes.utf8("next"), join(restarted.propose(Bytes.utf8("next"))));
    }

    @Test
    void transfersLeadershipToAFollower() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));
        String target = name(follower());

        join(leader.transferLeadership(Ids.id(target)));

        await(() -> runtimes.get(target).status().role() == Role.LEADER);
        assertEquals(Bytes.utf8("b"), join(runtimes.get(target).propose(Bytes.utf8("b"))));
    }

    @Test
    void stepsDownToAnotherVoter() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));

        join(leader.stepDown());

        await(() -> leader.status().role() != Role.LEADER && leader.status().leader().isPresent());
        var successor = runtimes.get(leader.status().leader().orElseThrow().value());
        assertEquals(Bytes.utf8("b"), join(successor.propose(Bytes.utf8("b"))));
    }

    @Test
    void rejectsTransferToANonVoter() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));

        assertInstanceOf(RaftException.InvalidRequest.class, failure(leader.transferLeadership(Ids.id("n9"))));
    }

    @Test
    void addsAndPromotesANewMember() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));
        start("n4", Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4")));

        var learner = join(leader.changeMembership(new MembershipChange.AddLearner(Ids.id("n4"))));
        assertTrue(learner.isLearner(Ids.id("n4")));
        var promoted = join(leader.changeMembership(new MembershipChange.PromoteToVoter(Ids.id("n4"))));
        assertTrue(promoted.isVoter(Ids.id("n4")));

        join(leader.propose(Bytes.utf8("b")));
        await(() -> machines.get("n4").commands.equals(List.of("a", "b")));
        await(() -> runtimes.get("n4").membership().isVoter(Ids.id("n4")));
    }

    @Test
    void rejectsAnInvalidMembershipChange() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));

        var error = failure(leader.changeMembership(new MembershipChange.PromoteToVoter(Ids.id("n2"))));

        assertInstanceOf(RaftException.InvalidRequest.class, error);
    }

    @Test
    void leaderRemovesItselfAndTheClusterMovesOn() throws Exception {
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("a")));
        NodeId removed = leader.status().nodeId();

        var membership = join(leader.changeMembership(new MembershipChange.RemoveNode(removed)));

        assertFalse(membership.isMember(removed));
        await(() -> runtimes.values().stream()
            .anyMatch(runtime -> runtime != leader && runtime.status().role() == Role.LEADER));
        var successor = runtimes.values().stream()
            .filter(runtime -> runtime != leader && runtime.status().role() == Role.LEADER)
            .findFirst().orElseThrow();
        assertEquals(Bytes.utf8("b"), join(successor.propose(Bytes.utf8("b"))));
        assertTrue(leader.status().role() != Role.LEADER);
    }

    @Test
    void isolatedLeaderStepsDownAndClusterMovesOn() throws Exception {
        startAll(THREE);
        var oldLeader = leader();
        String oldId = name(oldLeader);

        network.isolate(Ids.id(oldId));
        await(() -> oldLeader.status().role() != Role.LEADER);
        await(() -> runtimes.values().stream()
            .anyMatch(runtime -> runtime != oldLeader && runtime.status().role() == Role.LEADER));
        var newLeader = runtimes.values().stream()
            .filter(runtime -> runtime != oldLeader && runtime.status().role() == Role.LEADER)
            .findFirst().orElseThrow();
        join(newLeader.propose(Bytes.utf8("during-partition")));

        network.heal(Ids.id(oldId));
        await(() -> machines.get(oldId).commands.contains("during-partition"));
    }

    @Test
    void compactsTheLogOnceTheEntryThresholdIsReached() throws Exception {
        snapshots = SnapshotPolicy.everyEntries(20);
        startAll(THREE);
        var leader = leader();

        for (int i = 0; i < 100; i++) {
            join(leader.propose(Bytes.utf8("c" + i)));
        }

        await(() -> runtimes.values().stream().allMatch(runtime -> runtime.status().snapshotIndex() >= 80));
        var expected = machines.get(name(leader)).commands;
        await(() -> machines.values().stream().allMatch(machine -> machine.commands.equals(expected)));
    }

    @Test
    void compactsTheLogOnceTheByteThresholdIsReached() throws Exception {
        snapshots = new SnapshotPolicy(1_000_000, 4096);
        var runtime = start("n1", Ids.voters("n1"));
        await(() -> runtime.status().role() == Role.LEADER);

        String large = "x".repeat(1024);
        for (int i = 0; i < 8; i++) {
            join(runtime.propose(Bytes.utf8(large + i)));
        }

        await(() -> runtime.status().snapshotIndex() >= 4);
    }

    @Test
    void followerBehindCompactionCatchesUpFromLeaderSnapshot() throws Exception {
        snapshots = SnapshotPolicy.everyEntries(10);
        startAll(THREE);
        var leader = leader();
        join(leader.propose(Bytes.utf8("first")));
        String lagging = name(follower());
        network.isolate(Ids.id(lagging));

        for (int i = 0; i < 50; i++) {
            join(leader.propose(Bytes.utf8("c" + i)));
        }
        await(() -> leader.status().snapshotIndex() > 20);
        network.heal(Ids.id(lagging));

        var expected = machines.get(name(leader)).commands;
        await(() -> machines.get(lagging).commands.equals(expected));
        assertTrue(machines.get(lagging).restored != null);
    }

    @Test
    void restartRestoresFromSnapshotAndReplaysTheTail() throws Exception {
        snapshots = SnapshotPolicy.everyEntries(5);
        var single = Ids.voters("n1");
        var runtime = start("n1", single);
        await(() -> runtime.status().role() == Role.LEADER);
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

    private static final class RecordingStateMachine implements StateMachine<Bytes> {
        private final List<String> commands = new CopyOnWriteArrayList<>();
        private final List<Long> skipped = new CopyOnWriteArrayList<>();
        private volatile long applied;
        private volatile String restored;

        private RecordingStateMachine() {
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
        public synchronized void skip(long index) {
            skipped.add(index);
            applied = index;
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
