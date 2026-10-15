package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftEffects.ReadResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

final class ClusterHarness {

    private final Map<String, Raft> nodes = new LinkedHashMap<>();
    private final Map<String, InMemoryRaftLog> logs = new HashMap<>();
    private final Map<String, List<RaftLogEntry>> applied = new HashMap<>();
    private final Map<String, List<ReadResult>> reads = new HashMap<>();
    private final Deque<Envelope> inFlight = new ArrayDeque<>();
    private final Set<String> isolated = new HashSet<>();
    private final RaftOptions options;
    private long requests;

    private ClusterHarness(RaftOptions options) {
        this.options = options;
    }

    static String nodeId(int index) {
        return "n" + index;
    }

    static ClusterHarness create(int voters) {
        return create(voters, 0);
    }

    static ClusterHarness create(int voters, int learners) {
        var cluster = new ClusterHarness(RaftOptions.defaults());
        var voterIds = new HashSet<String>();
        var learnerIds = new HashSet<String>();
        for (int i = 0; i < voters + learners; i++) {
            (i < voters ? voterIds : learnerIds).add(nodeId(i));
        }
        var membership = RaftMembership.of(voterIds, learnerIds);
        for (int i = 0; i < voters + learners; i++) {
            cluster.add(nodeId(i), membership, i);
        }
        return cluster;
    }

    void add(String id, RaftMembership membership, int jitter) {
        var log = new InMemoryRaftLog();
        logs.put(id, log);
        applied.put(id, new ArrayList<>());
        reads.put(id, new ArrayList<>());
        nodes.put(id, Raft.recover(id, membership, options, log, RaftHardState.INITIAL, 0, TestNode.jitter(jitter)));
    }

    Raft node(String id) {
        return nodes.get(id);
    }

    Raft node(int index) {
        return node(nodeId(index));
    }

    List<Raft> nodes() {
        return List.copyOf(nodes.values());
    }

    InMemoryRaftLog log(String id) {
        return logs.get(id);
    }

    List<RaftLogEntry> applied(String id) {
        return List.copyOf(applied.get(id));
    }

    List<RaftLogEntry.Command> appliedCommands(String id) {
        return applied.get(id).stream()
            .filter(RaftLogEntry.Command.class::isInstance)
            .map(RaftLogEntry.Command.class::cast)
            .toList();
    }

    List<ReadResult> drainReads(String id) {
        var result = List.copyOf(reads.get(id));
        reads.get(id).clear();
        return result;
    }

    Optional<String> leader() {
        return nodes.entrySet().stream()
            .filter(entry -> !isolated.contains(entry.getKey()) && entry.getValue().role() == RaftRole.LEADER)
            .max(java.util.Comparator.comparingLong(entry -> entry.getValue().term()))
            .map(Map.Entry::getKey);
    }

    long leaderCount(long term) {
        return nodes.values().stream().filter(raft -> raft.role() == RaftRole.LEADER && raft.term() == term).count();
    }

    void step(String id, RaftInput input) {
        nodes.get(id).step(input);
        drain(id);
    }

    void tick() {
        for (String id : List.copyOf(nodes.keySet())) {
            step(id, new RaftInput.Tick());
        }
    }

    void deliverAll() {
        while (!inFlight.isEmpty()) {
            var envelope = inFlight.pollFirst();
            if (isolated.contains(envelope.from()) || isolated.contains(envelope.to()) || !nodes.containsKey(envelope.to())) {
                continue;
            }
            step(envelope.to(), new RaftInput.Receive(envelope.from(), envelope.message()));
        }
    }

    void run(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tick();
            deliverAll();
        }
    }

    void runUntil(Predicate<ClusterHarness> condition, int maxTicks) {
        for (int i = 0; i < maxTicks && !condition.test(this); i++) {
            tick();
            deliverAll();
        }
        if (!condition.test(this)) {
            throw new AssertionError("condition not met within " + maxTicks + " ticks");
        }
    }

    String electLeader() {
        runUntil(cluster -> cluster.leader().isPresent(), 100);
        return leader().orElseThrow();
    }

    void isolate(String id) {
        isolated.add(id);
    }

    void heal(String id) {
        isolated.remove(id);
    }

    long propose(String id, String command) {
        long request = ++requests;
        step(id, new RaftInput.Propose(request, Bytes.utf8(command)));
        return request;
    }

    long readIndex(String id) {
        long request = ++requests;
        step(id, new RaftInput.ReadIndex(request));
        return request;
    }

    void changeMembership(String id, MembershipChange change) {
        step(id, new RaftInput.ChangeMembership(++requests, change));
    }

    void compact(String id) {
        var raft = nodes.get(id);
        var log = logs.get(id);
        long index = raft.appliedIndex();
        log.compact(new SnapshotMetadata(index, log.term(index), raft.membership()));
    }

    private void drain(String id) {
        var raft = nodes.get(id);
        var log = logs.get(id);
        for (var effects = raft.ready(); !effects.isEmpty(); effects = raft.ready()) {
            if (effects.snapshot().isPresent()) {
                applied.get(id).clear();
            }
            log.persist(effects);
            inFlight.addAll(effects.messages());
            applied.get(id).addAll(effects.committed());
            reads.get(id).addAll(effects.reads());
            for (String peer : effects.snapshotRequests()) {
                var snapshot = log.snapshot().orElseThrow();
                inFlight.addLast(new Envelope(id, peer, new RaftMessage.InstallSnapshot(raft.term(), snapshot)));
                raft.step(new RaftInput.SnapshotSent(peer));
            }
            effects.persisted().ifPresent(raft::step);
        }
    }
}
