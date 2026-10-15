package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.LinearizabilityChecker.Operation;
import io.partdb.raft.RaftEffects.ProposalResult;
import io.partdb.raft.RaftEffects.ReadResult;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

final class Simulation {

    record Settings(
        int nodes,
        int ticks,
        int clients,
        int keys,
        double dropRate,
        double duplicateRate,
        int maxDelay,
        double partitionRate,
        double crashRate,
        double transferRate,
        int compactionThreshold
    ) {
        static Settings forSeed(long seed) {
            var random = new SplittableRandom(~seed);
            return new Settings(
                random.nextBoolean() ? 3 : 5,
                2000,
                2 + random.nextInt(5),
                1 + random.nextInt(4),
                random.nextDouble(0.2),
                random.nextDouble(0.05),
                1 + random.nextInt(4),
                random.nextDouble(0.01),
                random.nextDouble(0.006),
                random.nextDouble(0.01),
                10 + random.nextInt(60)
            );
        }
    }

    record Report(int completedWrites, int completedReads, long maxTerm, long commitIndex) {}

    private static final int CLIENT_TIMEOUT_TICKS = 200;
    private static final int CONVERGENCE_TICKS = 2000;

    private final long seed;
    private final Settings settings;
    private final SplittableRandom random;
    private final RaftOptions options = RaftOptions.defaults();
    private final RaftMembership membership;
    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final PriorityQueue<Envelope> network = new PriorityQueue<>(
        Comparator.comparingLong(Envelope::deliverAt).thenComparingLong(Envelope::sequence));
    private final List<Client> clients = new ArrayList<>();
    private final List<Operation> history = new ArrayList<>();
    private final List<PendingWrite> indeterminate = new ArrayList<>();
    private final Map<Long, String> leaders = new HashMap<>();
    private final Map<Long, RaftLogEntry> committed = new HashMap<>();
    private final Map<String, Long> firstApplied = new HashMap<>();

    private PrintStream trace;
    private long now;
    private long clock;
    private long sequence;
    private long requestIds;
    private Set<String> partitioned = Set.of();
    private long partitionEnds;
    private int completedWrites;
    private int completedReads;

    Simulation(long seed, Settings settings) {
        this.seed = seed;
        this.settings = settings;
        this.random = new SplittableRandom(seed);
        var voters = new HashSet<String>();
        for (int i = 0; i < settings.nodes(); i++) {
            voters.add("n" + i);
        }
        this.membership = RaftMembership.of(voters, Set.of());
        for (String id : membership.voters()) {
            nodes.put(id, new Node(id));
        }
        for (int i = 0; i < settings.clients(); i++) {
            clients.add(new Client(i));
        }
    }

    Simulation trace(PrintStream out) {
        this.trace = out;
        return this;
    }

    Report run() {
        nodes.values().forEach(this::boot);
        for (; now < settings.ticks(); now++) {
            injectFaults();
            issueOperations();
            tickNodes();
            deliverMessages();
            compactLogs();
            expireOperations();
        }
        converge();
        resolveIndeterminateWrites();
        verifyLogMatching();
        LinearizabilityChecker.violation(history).ifPresent(this::fail);
        return new Report(
            completedWrites,
            completedReads,
            leaders.keySet().stream().mapToLong(Long::longValue).max().orElse(0),
            nodes.values().stream().mapToLong(node -> node.raft.commitIndex()).max().orElse(0)
        );
    }

    private void boot(Node node) {
        var snapshot = node.log.snapshot();
        long snapshotIndex = snapshot.map(SnapshotMetadata::index).orElse(0L);
        node.raft = Raft.recover(
            node.id,
            snapshot.map(SnapshotMetadata::membership).orElse(membership),
            options,
            node.log,
            node.log.hardState(),
            snapshotIndex,
            random.split()
        );
        node.kv = new HashMap<>(node.snapshotState);
        node.applied = snapshotIndex;
        node.membershipAtApplied = snapshot.map(SnapshotMetadata::membership).orElse(membership);
        node.lastCommit = node.raft.commitIndex();
        node.up = true;
        drain(node);
    }

    private void crash(Node node) {
        node.up = false;
        node.downUntil = now + 10 + random.nextInt(80);
        node.raft = null;
        node.kv = null;
        node.incomingSnapshots.clear();
        node.inbox.clear();
        node.proposals.values().forEach(this::abandon);
        node.proposals.clear();
        node.writes.values().forEach(this::abandon);
        node.writes.clear();
        node.reads.values().forEach(read -> read.client().pending = null);
        node.reads.clear();
        node.readIndexes.clear();
    }

    private void injectFaults() {
        if (!partitioned.isEmpty() && now >= partitionEnds) {
            partitioned = Set.of();
        } else if (partitioned.isEmpty() && random.nextDouble() < settings.partitionRate()) {
            var ids = new ArrayList<>(nodes.keySet());
            int size = 1 + random.nextInt(ids.size() / 2);
            var minority = new HashSet<String>();
            while (minority.size() < size) {
                minority.add(ids.get(random.nextInt(ids.size())));
            }
            partitioned = Set.copyOf(minority);
            partitionEnds = now + 20 + random.nextInt(100);
        }

        for (Node node : nodes.values()) {
            if (!node.up && now >= node.downUntil) {
                boot(node);
            }
        }
        if (random.nextDouble() < settings.crashRate()) {
            var up = nodes.values().stream().filter(node -> node.up).toList();
            if (!up.isEmpty()) {
                crash(up.get(random.nextInt(up.size())));
            }
        }
        if (random.nextDouble() < settings.transferRate()) {
            leader().ifPresent(leader -> {
                var voters = List.copyOf(leader.raft.membership().voters());
                step(leader, new RaftInput.TransferLeadership(voters.get(random.nextInt(voters.size()))));
            });
        }
    }

    private Optional<Node> leader() {
        return nodes.values().stream()
            .filter(node -> node.up && node.raft.role() == RaftRole.LEADER)
            .max(Comparator.comparingLong(node -> node.raft.term()));
    }

    private void issueOperations() {
        for (Client client : clients) {
            if (client.pending != null || random.nextDouble() >= 0.3) {
                continue;
            }
            Node node = chooseNode();
            if (!node.up) {
                continue;
            }
            String key = "k" + random.nextInt(settings.keys());
            if (random.nextBoolean()) {
                write(client, node, key);
            } else {
                read(client, node, key);
            }
        }
    }

    private Node chooseNode() {
        if (random.nextDouble() < 0.7) {
            var leader = leader();
            if (leader.isPresent()) {
                return leader.get();
            }
        }
        var all = List.copyOf(nodes.values());
        return all.get(random.nextInt(all.size()));
    }

    private void write(Client client, Node node, String key) {
        long id = ++requestIds;
        var pending = new PendingWrite(client, node, key, "c" + client.id + "-" + client.counter++, ++clock, now, id);
        node.proposals.put(id, pending);
        client.pending = pending;
        step(node, new RaftInput.Propose(id, Bytes.utf8(key + "=" + pending.value())));
    }

    private void read(Client client, Node node, String key) {
        long id = ++requestIds;
        var pending = new PendingRead(client, node, key, ++clock, now, id);
        node.reads.put(id, pending);
        client.pending = pending;
        step(node, new RaftInput.ReadIndex(id));
    }

    private void tickNodes() {
        for (Node node : nodes.values()) {
            if (!node.up) {
                continue;
            }
            drainInbox(node);
            if (random.nextDouble() < 0.95) {
                step(node, new RaftInput.Tick());
            }
        }
    }

    private void drainInbox(Node node) {
        while (!node.inbox.isEmpty()) {
            step(node, node.inbox.poll());
        }
    }

    private void deliverMessages() {
        while (!network.isEmpty() && network.peek().deliverAt() <= now) {
            var envelope = network.poll();
            Node target = nodes.get(envelope.to());
            if (!target.up || partitioned.contains(envelope.from()) != partitioned.contains(envelope.to())) {
                continue;
            }
            if (envelope.message() instanceof RaftMessage.InstallSnapshot install) {
                target.incomingSnapshots.put(install.snapshot().index(), envelope.snapshot());
            }
            step(target, new RaftInput.Receive(envelope.from(), envelope.message()));
        }
    }

    private void compactLogs() {
        for (Node node : nodes.values()) {
            long snapshotIndex = node.log.snapshot().map(SnapshotMetadata::index).orElse(0L);
            if (node.up && node.applied - snapshotIndex > settings.compactionThreshold()) {
                var metadata = new SnapshotMetadata(node.applied, node.log.term(node.applied), node.membershipAtApplied);
                node.snapshotState = Map.copyOf(node.kv);
                node.log.compact(metadata);
            }
        }
    }

    private void expireOperations() {
        for (Client client : clients) {
            switch (client.pending) {
                case PendingWrite write when now - write.issuedAt() > CLIENT_TIMEOUT_TICKS -> {
                    write.node().proposals.remove(write.id());
                    write.node().writes.values().remove(write);
                    abandon(write);
                }
                case PendingRead read when now - read.issuedAt() > CLIENT_TIMEOUT_TICKS -> {
                    read.node().reads.remove(read.id());
                    read.node().readIndexes.remove(read.id());
                    client.pending = null;
                }
                case null, default -> {}
            }
        }
    }

    private void converge() {
        partitioned = Set.of();
        for (Node node : nodes.values()) {
            if (!node.up) {
                boot(node);
            }
        }
        for (int i = 0; i < CONVERGENCE_TICKS; i++, now++) {
            for (Node node : nodes.values()) {
                drainInbox(node);
                step(node, new RaftInput.Tick());
            }
            deliverMessages();
            if (converged()) {
                nodes.values().forEach(node -> {
                    node.proposals.values().forEach(this::abandon);
                    node.writes.values().forEach(this::abandon);
                });
                return;
            }
        }
        fail("cluster did not converge after faults healed");
    }

    private boolean converged() {
        var leader = leader();
        if (leader.isEmpty()) {
            return false;
        }
        var raft = leader.get().raft;
        long commit = raft.commitIndex();
        if (commit == 0 || leader.get().log.term(commit) != raft.term()) {
            return false;
        }
        return nodes.values().stream().allMatch(node -> node.raft.commitIndex() == commit && node.applied == commit);
    }

    private void resolveIndeterminateWrites() {
        for (PendingWrite write : indeterminate) {
            Long applied = firstApplied.get(write.value());
            if (applied != null) {
                history.add(Operation.write(write.key(), write.value(), write.call(), applied));
            }
        }
    }

    private void verifyLogMatching() {
        var logs = nodes.values().stream()
            .map(node -> node.log.entries().stream().collect(Collectors.toMap(RaftLogEntry::index, entry -> entry)))
            .toList();
        for (int a = 0; a < logs.size(); a++) {
            for (int b = a + 1; b < logs.size(); b++) {
                var left = logs.get(a);
                var right = logs.get(b);
                long highestMatch = left.keySet().stream()
                    .filter(index -> right.containsKey(index) && right.get(index).term() == left.get(index).term())
                    .mapToLong(Long::longValue)
                    .max()
                    .orElse(0);
                for (var entry : left.entrySet()) {
                    long index = entry.getKey();
                    if (index <= highestMatch && right.containsKey(index) && !right.get(index).equals(entry.getValue())) {
                        fail("logs diverge at index " + index + " below matching index " + highestMatch);
                    }
                }
            }
        }
    }

    private void step(Node node, RaftInput input) {
        if (trace != null && !(input instanceof RaftInput.Tick)) {
            trace.printf("%5d %s %-13s t=%d c=%d a=%d log=[%d..%d] <- %s%n", now, node.id, node.raft.role(), node.raft.term(),
                node.raft.commitIndex(), node.raft.appliedIndex(), node.log.firstIndex(), node.log.lastIndex(), input);
        }
        node.raft.step(input);
        drain(node);
    }

    private void drain(Node node) {
        for (var effects = node.raft.ready(); !effects.isEmpty(); effects = node.raft.ready()) {
            observe(node);
            effects.snapshot().ifPresent(metadata -> restoreSnapshot(node, metadata));
            node.log.persist(effects);
            for (var envelope : effects.messages()) {
                send(envelope.from(), envelope.to(), envelope.message(), null);
            }
            effects.proposals().forEach(result -> resolveProposal(node, result));
            effects.committed().forEach(entry -> apply(node, entry));
            effects.reads().forEach(result -> resolveRead(node, result));
            serveReads(node);
            effects.snapshotRequests().forEach(peer -> sendSnapshot(node, peer));
            effects.persisted().ifPresent(node.raft::step);
        }
        observe(node);
    }

    private void restoreSnapshot(Node node, SnapshotMetadata metadata) {
        var payload = node.incomingSnapshots.remove(metadata.index());
        if (payload == null) {
            fail("snapshot " + metadata.index() + " accepted without a payload");
        }
        node.snapshotState = payload;
        node.kv = new HashMap<>(payload);
        node.applied = metadata.index();
        node.membershipAtApplied = metadata.membership();
        resolveWritesThrough(node, metadata.index());
    }

    private void resolveProposal(Node node, ProposalResult result) {
        var write = node.proposals.remove(result.id());
        if (write == null) {
            return;
        }
        switch (result) {
            case ProposalResult.Accepted(_, long index, long entryTerm) -> node.writes.put(index, write.at(index, entryTerm));
            case ProposalResult.Rejected _ -> write.client().pending = null;
        }
    }

    private void apply(Node node, RaftLogEntry entry) {
        if (entry.index() != node.applied + 1) {
            fail(node.id + " applied index " + entry.index() + " after " + node.applied);
        }
        var previous = committed.putIfAbsent(entry.index(), entry);
        if (previous != null && !previous.equals(entry)) {
            fail("index " + entry.index() + " committed as " + previous + " and " + entry);
        }
        node.applied = entry.index();
        switch (entry) {
            case RaftLogEntry.Command command -> {
                String text = command.command().utf8();
                int separator = text.indexOf('=');
                String value = text.substring(separator + 1);
                node.kv.put(text.substring(0, separator), value);
                firstApplied.putIfAbsent(value, ++clock);
            }
            case RaftLogEntry.Membership change -> node.membershipAtApplied = change.membership();
            case RaftLogEntry.NoOp _ -> {}
        }

        var write = node.writes.remove(entry.index());
        if (write != null) {
            if (write.term() == entry.term()) {
                history.add(Operation.write(write.key(), write.value(), write.call(), ++clock));
                completedWrites++;
            }
            write.client().pending = null;
        }
    }

    private void resolveRead(Node node, ReadResult result) {
        var read = node.reads.get(result.id());
        if (read == null) {
            return;
        }
        switch (result) {
            case ReadResult.Confirmed(_, long index) -> node.readIndexes.put(read.id(), index);
            case ReadResult.Rejected _ -> {
                node.reads.remove(read.id());
                read.client().pending = null;
            }
        }
    }

    private void resolveWritesThrough(Node node, long index) {
        var iterator = node.writes.values().iterator();
        while (iterator.hasNext()) {
            var write = iterator.next();
            if (write.index() <= index) {
                iterator.remove();
                abandon(write);
            }
        }
    }

    private void serveReads(Node node) {
        var iterator = node.reads.values().iterator();
        while (iterator.hasNext()) {
            var read = iterator.next();
            Long readIndex = node.readIndexes.get(read.id());
            if (readIndex != null && node.applied >= readIndex) {
                iterator.remove();
                node.readIndexes.remove(read.id());
                history.add(Operation.read(read.key(), node.kv.get(read.key()), read.call(), ++clock));
                completedReads++;
                read.client().pending = null;
            }
        }
    }

    private void abandon(PendingWrite write) {
        indeterminate.add(write);
        write.client().pending = null;
    }

    private void sendSnapshot(Node node, String peer) {
        var snapshot = node.log.snapshot().orElseThrow(() -> new AssertionError("snapshot requested without a snapshot"));
        boolean delivered = send(node.id, peer, new RaftMessage.InstallSnapshot(node.raft.term(), snapshot), node.snapshotState);
        node.inbox.add(delivered ? new RaftInput.SnapshotSent(peer) : new RaftInput.SnapshotFailed(peer));
    }

    private boolean send(String from, String to, RaftMessage message, Map<String, String> snapshot) {
        if (random.nextDouble() < settings.dropRate()) {
            if (random.nextBoolean()) {
                nodes.get(from).inbox.add(new RaftInput.Unreachable(to));
            }
            return false;
        }
        network.add(new Envelope(now + 1 + random.nextInt(settings.maxDelay()), sequence++, from, to, message, snapshot));
        if (random.nextDouble() < settings.duplicateRate()) {
            network.add(new Envelope(now + 1 + random.nextInt(settings.maxDelay()), sequence++, from, to, message, snapshot));
        }
        return true;
    }

    private void observe(Node node) {
        var raft = node.raft;
        if (raft.role() == RaftRole.LEADER) {
            String existing = leaders.putIfAbsent(raft.term(), node.id);
            if (existing != null && !existing.equals(node.id)) {
                fail("term " + raft.term() + " has two leaders: " + existing + " and " + node.id);
            }
        }
        if (raft.commitIndex() < node.lastCommit) {
            fail(node.id + " commit index decreased from " + node.lastCommit + " to " + raft.commitIndex());
        }
        node.lastCommit = raft.commitIndex();
    }

    private void fail(String message) {
        throw new AssertionError("seed " + seed + " at tick " + now + ": " + message);
    }

    private record Envelope(long deliverAt, long sequence, String from, String to, RaftMessage message, Map<String, String> snapshot) {}

    private sealed interface Pending permits PendingWrite, PendingRead {}

    private record PendingWrite(
        Client client,
        Node node,
        String key,
        String value,
        long call,
        long issuedAt,
        long id,
        long index,
        long term
    ) implements Pending {
        PendingWrite(Client client, Node node, String key, String value, long call, long issuedAt, long id) {
            this(client, node, key, value, call, issuedAt, id, 0, 0);
        }

        PendingWrite at(long index, long term) {
            return new PendingWrite(client, node, key, value, call, issuedAt, id, index, term);
        }
    }

    private record PendingRead(Client client, Node node, String key, long call, long issuedAt, long id) implements Pending {}

    private static final class Client {
        private final int id;
        private int counter;
        private Pending pending;

        private Client(int id) {
            this.id = id;
        }
    }

    private final class Node {
        private final String id;
        private final InMemoryRaftLog log = new InMemoryRaftLog();
        private final Map<Long, PendingWrite> proposals = new HashMap<>();
        private final Map<Long, PendingWrite> writes = new HashMap<>();
        private final Map<Long, PendingRead> reads = new LinkedHashMap<>();
        private final Map<Long, Long> readIndexes = new HashMap<>();
        private final Map<Long, Map<String, String>> incomingSnapshots = new HashMap<>();
        private final Queue<RaftInput> inbox = new ArrayDeque<>();
        private Map<String, String> snapshotState = Map.of();
        private RaftMembership membershipAtApplied;
        private Raft raft;
        private Map<String, String> kv;
        private long applied;
        private long lastCommit;
        private boolean up;
        private long downUntil;

        private Node(String id) {
            this.id = id;
        }
    }
}
