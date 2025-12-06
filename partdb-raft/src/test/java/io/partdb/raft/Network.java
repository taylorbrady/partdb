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

final class Network {
    private final Map<String, Raft> nodes = new LinkedHashMap<>();
    private final Deque<Envelope> inFlight = new ArrayDeque<>();
    private final Set<String> isolated = new HashSet<>();
    private final List<ReadState> readStates = new ArrayList<>();
    private final List<MembershipChangeEvent> membershipChanges = new ArrayList<>();

    private record Envelope(String from, String to, RaftMessage message) {}

    record ReadState(String nodeId, long index, byte[] context) {}

    record MembershipChangeEvent(String nodeId, long index, Membership previous, Membership current) {}

    private Network() {}

    static String nodeId(int index) {
        return "node-" + index;
    }

    static Network create(int voterCount) {
        return create(voterCount, 0, RaftConfig.defaults());
    }

    static Network create(int voterCount, int learnerCount) {
        return create(voterCount, learnerCount, RaftConfig.defaults());
    }

    static Network create(int voterCount, int learnerCount, RaftConfig config) {
        var network = new Network();

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
            var storage = new InMemoryStorage(membership);
            var raft = new Raft(nodeId, membership, config, storage, _ -> jitter);
            network.nodes.put(nodeId, raft);
        }
        for (String nodeId : learners) {
            int jitter = nodeIndex++;
            var storage = new InMemoryStorage(membership);
            var raft = new Raft(nodeId, membership, config, storage, _ -> jitter);
            network.nodes.put(nodeId, raft);
        }

        return network;
    }

    void tick() {
        for (var nodeId : nodes.keySet()) {
            tickNode(nodeId);
        }
    }

    void tickNode(String nodeId) {
        var raft = nodes.get(nodeId);
        if (raft != null) {
            var ready = raft.step(new RaftEvent.Tick());
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

        var raft = nodes.get(envelope.to);
        if (raft != null) {
            var ready = raft.step(new RaftEvent.Receive(envelope.from, envelope.message));
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
                    var raft = nodes.get(to);
                    if (raft != null) {
                        var ready = raft.step(new RaftEvent.Receive(from, envelope.message()));
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
        return nodes.get(id);
    }

    Raft node(int index) {
        return nodes.get(nodeId(index));
    }

    List<Raft> allNodes() {
        return List.copyOf(nodes.values());
    }

    Optional<String> leader() {
        return nodes.entrySet().stream()
            .filter(e -> e.getValue().isLeader())
            .map(Map.Entry::getKey)
            .findFirst();
    }

    long leaderCount() {
        return nodes.values().stream()
            .filter(Raft::isLeader)
            .count();
    }

    void propose(String nodeId, byte[] data) {
        var raft = nodes.get(nodeId);
        if (raft != null) {
            var ready = raft.step(new RaftEvent.Propose(data));
            processReady(nodeId, ready);
        }
    }

    void readIndex(String nodeId, byte[] context) {
        var raft = nodes.get(nodeId);
        if (raft != null) {
            var ready = raft.step(new RaftEvent.ReadIndex(context));
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
        var raft = nodes.get(nodeId);
        if (raft != null) {
            var ready = raft.step(new RaftEvent.ChangeConfig(change));
            processReady(nodeId, ready);
        }
    }

    List<MembershipChangeEvent> drainMembershipChanges() {
        var result = List.copyOf(membershipChanges);
        membershipChanges.clear();
        return result;
    }

    void addNode(String nodeId, Raft raft) {
        nodes.put(nodeId, raft);
    }

    void runUntil(Predicate<Network> condition, int maxTicks) {
        for (int i = 0; i < maxTicks && !condition.test(this); i++) {
            tick();
            deliverAll();
        }
    }

    private void processReady(String from, Ready ready) {
        for (var outbound : ready.messages()) {
            inFlight.addLast(new Envelope(from, outbound.to(), outbound.message()));
        }

        for (var readState : ready.apply().readStates()) {
            readStates.add(new ReadState(from, readState.index(), readState.context()));
        }

        for (var change : ready.apply().membershipChanges()) {
            membershipChanges.add(new MembershipChangeEvent(from, change.index(), change.previous(), change.current()));
        }

        var snapshot = ready.snapshotToSend();
        if (snapshot != null) {
            var node = nodes.get(from);
            var msg = new RaftMessage.InstallSnapshot(
                node.term(),
                from,
                snapshot.index(),
                snapshot.term(),
                node.membership(),
                new byte[0]
            );
            inFlight.addLast(new Envelope(from, snapshot.peer(), msg));
        }
    }
}
