package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static io.partdb.raft.Ids.id;

final class ClusterHarness {

    private record SnapshotDelivery(NodeId from, NodeId to, long term, SnapshotMetadata snapshot) {}

    private final Map<NodeId, Raft> nodes = new LinkedHashMap<>();
    private final Map<NodeId, InMemoryRaftLog> logs = new HashMap<>();
    private final Map<NodeId, List<LogEntry>> applied = new HashMap<>();
    private final Map<NodeId, List<Outcome>> outcomes = new HashMap<>();
    private final Deque<Object> inFlight = new ArrayDeque<>();
    private final Set<NodeId> isolated = new HashSet<>();
    private final Options options;
    private long requests;

    private ClusterHarness(Options options) {
        this.options = options;
    }

    static String nodeId(int index) {
        return "n" + index;
    }

    static ClusterHarness create(int voters) {
        return create(voters, 0);
    }

    static ClusterHarness create(int voters, int learners) {
        var cluster = new ClusterHarness(Options.defaults());
        var voterIds = new HashSet<String>();
        var learnerIds = new HashSet<String>();
        for (int i = 0; i < voters + learners; i++) {
            (i < voters ? voterIds : learnerIds).add(nodeId(i));
        }
        var membership = Ids.membership(voterIds, learnerIds);
        for (int i = 0; i < voters + learners; i++) {
            cluster.add(nodeId(i), membership, i);
        }
        return cluster;
    }

    void add(String name, Membership membership, int jitter) {
        var node = id(name);
        var log = new InMemoryRaftLog();
        logs.put(node, log);
        applied.put(node, new ArrayList<>());
        outcomes.put(node, new ArrayList<>());
        nodes.put(node, Raft.recover(node, membership, options, log, HardState.INITIAL, 0, TestNode.jitter(jitter)));
    }

    Raft node(String name) {
        return nodes.get(id(name));
    }

    Raft node(int index) {
        return node(nodeId(index));
    }

    List<Raft> nodes() {
        return List.copyOf(nodes.values());
    }

    InMemoryRaftLog log(String name) {
        return logs.get(id(name));
    }

    List<LogEntry> applied(String name) {
        return List.copyOf(applied.get(id(name)));
    }

    List<LogEntry.Command> appliedCommands(String name) {
        return applied.get(id(name)).stream()
            .filter(LogEntry.Command.class::isInstance)
            .map(LogEntry.Command.class::cast)
            .toList();
    }

    List<Outcome> drainOutcomes(String name) {
        var list = outcomes.get(id(name));
        var result = List.copyOf(list);
        list.clear();
        return result;
    }

    Optional<String> leader() {
        return nodes.entrySet().stream()
            .filter(entry -> !isolated.contains(entry.getKey()) && entry.getValue().role() == Role.LEADER)
            .max(Comparator.comparingLong(entry -> entry.getValue().term()))
            .map(entry -> entry.getKey().value());
    }

    long leaderCount(long term) {
        return nodes.values().stream().filter(raft -> raft.role() == Role.LEADER && raft.term() == term).count();
    }

    void step(String name, Input input) {
        step(id(name), input);
    }

    private void step(NodeId node, Input input) {
        nodes.get(node).step(input);
        drain(node);
    }

    void tick() {
        for (NodeId node : List.copyOf(nodes.keySet())) {
            step(node, new Input.Tick());
        }
    }

    void deliverAll() {
        while (!inFlight.isEmpty()) {
            switch (inFlight.pollFirst()) {
                case Envelope envelope when reachable(envelope.from(), envelope.to()) ->
                    step(envelope.to(), new Input.Receive(envelope.from(), envelope.message()));
                case SnapshotDelivery delivery when reachable(delivery.from(), delivery.to()) ->
                    step(delivery.to(), new Input.InstallSnapshot(delivery.from(), delivery.term(), delivery.snapshot()));
                default -> {}
            }
        }
    }

    private boolean reachable(NodeId from, NodeId to) {
        return !isolated.contains(from) && !isolated.contains(to) && nodes.containsKey(to);
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

    void isolate(String name) {
        isolated.add(id(name));
    }

    void heal(String name) {
        isolated.remove(id(name));
    }

    long propose(String name, String command) {
        long request = ++requests;
        step(name, new Input.Propose(request, Bytes.utf8(command)));
        return request;
    }

    long readIndex(String name) {
        long request = ++requests;
        step(name, new Input.ReadIndex(request));
        return request;
    }

    long changeMembership(String name, MembershipChange change) {
        long request = ++requests;
        step(name, new Input.ChangeMembership(request, change));
        return request;
    }

    long transferLeadership(String name, String target) {
        long request = ++requests;
        step(name, new Input.TransferLeadership(request, id(target)));
        return request;
    }

    void compact(String name) {
        var raft = node(name);
        var log = log(name);
        long index = raft.appliedIndex();
        log.compact(new SnapshotMetadata(index, log.term(index), raft.membershipAt(index)));
    }

    private void drain(NodeId node) {
        var raft = nodes.get(node);
        var log = logs.get(node);
        for (var ready = raft.ready(); !ready.isEmpty(); ready = raft.ready()) {
            if (ready.snapshot().isPresent()) {
                applied.get(node).clear();
            }
            log.persist(ready);
            inFlight.addAll(ready.messages());
            applied.get(node).addAll(ready.committed());
            outcomes.get(node).addAll(ready.outcomes());
            for (NodeId peer : ready.snapshotRequests()) {
                var snapshot = log.snapshot().orElseThrow();
                inFlight.addLast(new SnapshotDelivery(node, peer, raft.term(), snapshot));
                raft.step(new Input.SnapshotSent(peer));
            }
            ready.persisted().ifPresent(raft::step);
        }
    }
}
