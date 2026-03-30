package io.partdb.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

final class ClusterHarness {
    private final Map<String, SimulatedNode> nodes = new LinkedHashMap<>();
    private final Deque<Envelope> inFlight = new ArrayDeque<>();
    private final Set<String> isolated = new HashSet<>();
    private final List<ReadState> readStates = new ArrayList<>();
    private final List<MembershipChangeEvent> membershipChanges = new ArrayList<>();

    private record SimulatedNode(Raft raft, InMemoryStorage storage) {}

    private record Envelope(String from, String to, RaftMessage message) {}

    record ReadState(String nodeId, long index, byte[] context) {
        ReadState {
            context = context.clone();
        }

        @Override
        public byte[] context() {
            return context.clone();
        }
    }

    record MembershipChangeEvent(String nodeId, long index, Membership previous, Membership current) {}

    private ClusterHarness() {}

    static String nodeId(int index) {
        return "node-" + index;
    }

    static ClusterHarness create(int voterCount) {
        return create(voterCount, 0, RaftConfig.defaults());
    }

    static ClusterHarness create(int voterCount, int learnerCount) {
        return create(voterCount, learnerCount, RaftConfig.defaults());
    }

    static ClusterHarness create(int voterCount, int learnerCount, RaftConfig config) {
        var cluster = new ClusterHarness();

        var voters = new HashSet<String>();
        for (int i = 0; i < voterCount; i++) {
            voters.add(nodeId(i));
        }

        var learners = new HashSet<String>();
        for (int i = voterCount; i < voterCount + learnerCount; i++) {
            learners.add(nodeId(i));
        }

        var membership = new Membership(voters, learners);

        int nodeIndex = 0;
        for (String nodeId : voters) {
            int jitter = nodeIndex++;
            cluster.addNode(nodeId, membership, config, jitter);
        }
        for (String nodeId : learners) {
            int jitter = nodeIndex++;
            cluster.addNode(nodeId, membership, config, jitter);
        }

        return cluster;
    }

    void tick() {
        for (var nodeId : List.copyOf(nodes.keySet())) {
            tickNode(nodeId);
        }
    }

    void tickNode(String nodeId) {
        var node = nodes.get(nodeId);
        if (node != null) {
            var ready = node.raft().step(new RaftEvent.Tick());
            processReady(nodeId, ready);
        }
    }

    boolean deliverOne() {
        var envelope = inFlight.pollFirst();
        if (envelope == null) {
            return false;
        }

        if (isolated.contains(envelope.from) || isolated.contains(envelope.to)) {
            return true;
        }

        var node = nodes.get(envelope.to);
        if (node != null) {
            var ready = node.raft().step(new RaftEvent.Receive(envelope.from, envelope.message));
            processReady(envelope.to, ready);
        }
        return true;
    }

    void deliverAll() {
        while (deliverOne()) {}
    }

    boolean deliverBetween(String from, String to) {
        var it = inFlight.iterator();
        while (it.hasNext()) {
            var envelope = it.next();
            if (envelope.from().equals(from) && envelope.to().equals(to)) {
                it.remove();
                if (!isolated.contains(from) && !isolated.contains(to)) {
                    var node = nodes.get(to);
                    if (node != null) {
                        var ready = node.raft().step(new RaftEvent.Receive(from, envelope.message()));
                        processReady(to, ready);
                    }
                }
                return true;
            }
        }
        return false;
    }

    void isolate(String nodeId) {
        isolated.add(nodeId);
        inFlight.removeIf(e -> e.from.equals(nodeId) || e.to.equals(nodeId));
    }

    void heal(String nodeId) {
        isolated.remove(nodeId);
    }

    Raft node(String id) {
        var node = nodes.get(id);
        return node != null ? node.raft() : null;
    }

    Raft node(int index) {
        return node(nodeId(index));
    }

    List<Raft> allNodes() {
        return nodes.values().stream()
            .map(SimulatedNode::raft)
            .toList();
    }

    Optional<String> leader() {
        return nodes.entrySet().stream()
            .filter(e -> e.getValue().raft().isLeader())
            .map(Map.Entry::getKey)
            .findFirst();
    }

    long leaderCount() {
        return nodes.values().stream()
            .map(SimulatedNode::raft)
            .filter(Raft::isLeader)
            .count();
    }

    void propose(String nodeId, byte[] data) {
        var node = nodes.get(nodeId);
        if (node != null) {
            var ready = node.raft().step(new RaftEvent.Propose(data));
            processReady(nodeId, ready);
        }
    }

    void readIndex(String nodeId, byte[] context) {
        var node = nodes.get(nodeId);
        if (node != null) {
            var ready = node.raft().step(new RaftEvent.ReadIndex(context));
            processReady(nodeId, ready);
        }
    }

    List<ReadState> drainReadStates() {
        var result = List.copyOf(readStates);
        readStates.clear();
        return result;
    }

    boolean hasReadStates() {
        return !readStates.isEmpty();
    }

    void proposeConfigChange(String nodeId, ConfigChange change) {
        var node = nodes.get(nodeId);
        if (node != null) {
            var ready = node.raft().step(new RaftEvent.ChangeConfig(change));
            processReady(nodeId, ready);
        }
    }

    List<MembershipChangeEvent> drainMembershipChanges() {
        var result = List.copyOf(membershipChanges);
        membershipChanges.clear();
        return result;
    }

    void addNode(String nodeId, Raft raft, InMemoryStorage storage) {
        nodes.put(nodeId, new SimulatedNode(raft, storage));
    }

    void runUntil(Predicate<ClusterHarness> condition, int maxTicks) {
        for (int i = 0; i < maxTicks && !condition.test(this); i++) {
            tick();
            deliverAll();
        }
    }

    private void processReady(String from, Ready ready) {
        if (!ready.hasWork()) {
            return;
        }

        var node = nodes.get(from);
        if (node == null) {
            return;
        }

        // Mirror the node runtime contract: persist first, then send, then apply/advance.
        var persist = ready.persist();
        if (persist.hasWork()) {
            node.storage().append(persist.hardState(), persist.entries());
            if (persist.incomingSnapshot() != null) {
                node.storage().saveSnapshot(persist.incomingSnapshot());
            }
            if (persist.mustSync()) {
                node.storage().sync();
            }
        }

        for (var outbound : ready.messages()) {
            inFlight.addLast(new Envelope(from, outbound.to(), outbound.message()));
        }

        long lastAppliedIndex = 0;
        long lastAppliedTerm = 0;
        for (var change : ready.apply().membershipChanges()) {
            membershipChanges.add(new MembershipChangeEvent(from, change.index(), change.previous(), change.current()));
            if (change.index() > lastAppliedIndex) {
                lastAppliedIndex = change.index();
                lastAppliedTerm = node.storage().term(change.index());
            }
        }

        for (var entry : ready.apply().entries()) {
            lastAppliedIndex = entry.index();
            lastAppliedTerm = entry.term();
        }
        if (lastAppliedIndex > 0) {
            node.raft().advance(lastAppliedIndex, lastAppliedTerm);
        }

        for (var readState : ready.apply().readStates()) {
            readStates.add(new ReadState(from, readState.index(), readState.context()));
        }

        var snapshot = ready.snapshotToSend();
        if (snapshot != null) {
            var outgoingSnapshot = node.storage().snapshot()
                .orElseGet(() -> new Snapshot(snapshot.index(), snapshot.term(), node.raft().membership(), new byte[0]));
            var msg = new RaftMessage.InstallSnapshot(
                node.raft().term(),
                from,
                outgoingSnapshot.index(),
                outgoingSnapshot.term(),
                outgoingSnapshot.membership(),
                outgoingSnapshot.data()
            );
            inFlight.addLast(new Envelope(from, snapshot.peer(), msg));
        }
    }

    private void addNode(String nodeId, Membership membership, RaftConfig config, int jitter) {
        var storage = new InMemoryStorage(membership);
        var raft = Raft.builder(nodeId, membership, config, storage)
            .random(_ -> jitter)
            .build();
        addNode(nodeId, raft, storage);
    }
}
