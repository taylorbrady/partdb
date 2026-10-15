package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.LinearizabilityChecker.Operation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
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
    private final Map<Long, RaftEffects.ApplyEntry> committed = new HashMap<>();
    private final Map<String, Long> firstApplied = new HashMap<>();

    private java.io.PrintStream trace;
    private long now;
    private long clock;
    private long sequence;
    private long readIds;
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
        this.membership = new RaftMembership(voters, Set.of());
        for (int i = 0; i < settings.nodes(); i++) {
            var node = new Node("n" + i, new InMemoryRaftLog(membership));
            nodes.put(node.id, node);
        }
        for (int i = 0; i < settings.clients(); i++) {
            clients.add(new Client(i));
        }
    }

    Simulation trace(java.io.PrintStream out) {
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
        long snapshotIndex = node.log.snapshot().map(SnapshotMetadata::index).orElse(0L);
        node.raft = RaftNode.builder(node.id, membership, options, node.log)
            .electionJitter(random::nextInt)
            .restoredFrom(node.log.hardState(), snapshotIndex)
            .build();
        node.kv = new HashMap<>(node.snapshotState);
        node.applied = snapshotIndex;
        node.lastDataIndex = snapshotIndex;
        node.lastCommit = node.raft.commitIndex();
        node.up = true;
        step(node, new RaftInput.ReplayCommitted());
    }

    private void crash(Node node) {
        node.up = false;
        node.downUntil = now + 10 + random.nextInt(80);
        node.raft = null;
        node.kv = null;
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
            var leader = nodes.values().stream()
                .filter(node -> node.up && node.raft.isLeader())
                .max(Comparator.comparingLong(node -> node.raft.term()));
            if (leader.isPresent()) {
                return leader.get();
            }
        }
        var all = List.copyOf(nodes.values());
        return all.get(random.nextInt(all.size()));
    }

    private void write(Client client, Node node, String key) {
        if (!node.raft.isLeader()) {
            return;
        }
        String value = "c" + client.id + "-" + client.counter++;
        long call = ++clock;
        var effects = node.raft.step(new RaftInput.EntryProposed(Bytes.utf8(key + "=" + value)));
        var entries = effects.persistence().entries();
        if (entries.isEmpty()) {
            fail("leader accepted a proposal without appending an entry");
        }
        var entry = entries.getLast();
        var pending = new PendingWrite(client, node, key, value, call, now, entry.index(), entry.term());
        node.writes.put(entry.index(), pending);
        client.pending = pending;
        observe(node);
        process(node, effects);
    }

    private void read(Client client, Node node, String key) {
        long id = ++readIds;
        var pending = new PendingRead(client, node, key, ++clock, now, id);
        node.reads.put(id, pending);
        client.pending = pending;
        step(node, new RaftInput.ReadIndexRequested(id));
    }

    private void tickNodes() {
        for (Node node : nodes.values()) {
            if (node.up && random.nextDouble() < 0.95) {
                step(node, new RaftInput.Tick());
            }
        }
    }

    private void deliverMessages() {
        while (!network.isEmpty() && network.peek().deliverAt() <= now) {
            var envelope = network.poll();
            Node target = nodes.get(envelope.to());
            if (!target.up || partitioned.contains(envelope.from()) != partitioned.contains(envelope.to())) {
                continue;
            }
            target.incomingSnapshot = envelope.snapshot();
            step(target, new RaftInput.MessageReceived(envelope.from(), envelope.message()));
            target.incomingSnapshot = null;
        }
    }

    private void compactLogs() {
        for (Node node : nodes.values()) {
            long snapshotIndex = node.log.snapshot().map(SnapshotMetadata::index).orElse(0L);
            if (node.up && node.applied - snapshotIndex > settings.compactionThreshold()) {
                var metadata = new SnapshotMetadata(node.applied, node.log.term(node.applied), node.raft.membership());
                node.snapshotState = Map.copyOf(node.kv);
                node.log.compact(metadata);
            }
        }
    }

    private void expireOperations() {
        for (Client client : clients) {
            switch (client.pending) {
                case PendingWrite write when now - write.issuedAt() > CLIENT_TIMEOUT_TICKS -> {
                    write.node().writes.remove(write.index());
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
                step(node, new RaftInput.Tick());
            }
            deliverMessages();
            if (converged()) {
                nodes.values().forEach(node -> node.writes.values().forEach(this::abandon));
                return;
            }
        }
        fail("cluster did not converge after faults healed");
    }

    private boolean converged() {
        var leader = nodes.values().stream().filter(node -> node.raft.isLeader()).findFirst();
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
            String unstable;
            try {
                var field = RaftNode.class.getDeclaredField("unstable");
                field.setAccessible(true);
                var u = (UnstableLog) field.get(node.raft);
                unstable = u.offset() + "+" + u.entries().stream().map(e -> String.valueOf(e.index())).toList();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
            trace.printf("%5d %s %-9s t=%d c=%d log=[%d..%d] snap=%s u=%s <- %s%n", now, node.id, node.raft.role(), node.raft.term(),
                node.raft.commitIndex(), node.log.firstIndex(), node.log.lastIndex(),
                node.log.snapshot().map(SnapshotMetadata::index).orElse(0L), unstable, input);
        }
        var effects = node.raft.step(input);
        observe(node);
        process(node, effects);
    }

    private void process(Node node, RaftEffects effects) {
        if (!effects.hasWork()) {
            return;
        }
        var persistence = effects.persistence();
        if (persistence.hasWork()) {
            node.log.append(persistence.hardState().orElse(null), persistence.entries());
            persistence.incomingSnapshot().ifPresent(metadata -> installSnapshot(node, metadata));
            if (!persistence.entries().isEmpty()) {
                var last = persistence.entries().getLast();
                step(node, new RaftInput.EntriesPersisted(last.index(), last.term()));
            }
            if (persistence.incomingSnapshot().isPresent()) {
                step(node, new RaftInput.SnapshotPersisted());
            }
        }
        for (var outbound : effects.messages()) {
            send(node.id, outbound.to(), outbound.message(), null);
        }
        apply(node, effects.application());
        effects.snapshotNeeded().ifPresent(needed -> sendSnapshot(node, needed.peer()));
    }

    private void installSnapshot(Node node, SnapshotMetadata metadata) {
        var payload = node.incomingSnapshot;
        if (payload == null) {
            fail("snapshot accepted without a payload");
        }
        node.log.installSnapshot(metadata);
        node.snapshotState = payload;
        node.kv = new HashMap<>(payload);
        node.applied = metadata.index();
        node.lastDataIndex = metadata.index();
        resolveWritesThrough(node, metadata.index());
        serveReads(node);
    }

    private void apply(Node node, RaftEffects.Application application) {
        for (var entry : application.entries()) {
            var previous = committed.putIfAbsent(entry.index(), entry);
            if (previous != null && !previous.equals(entry)) {
                fail("index " + entry.index() + " applied as " + previous + " and " + entry);
            }
            if (entry.index() <= node.lastDataIndex) {
                continue;
            }
            node.lastDataIndex = entry.index();
            String command = entry.data().utf8();
            int separator = command.indexOf('=');
            String value = command.substring(separator + 1);
            node.kv.put(command.substring(0, separator), value);
            firstApplied.putIfAbsent(value, ++clock);

            var write = node.writes.remove(entry.index());
            if (write != null) {
                if (write.term() == entry.term()) {
                    history.add(Operation.write(write.key(), write.value(), write.call(), ++clock));
                    completedWrites++;
                }
                write.client().pending = null;
            }
        }

        if (application.appliedThroughIndex() > 0) {
            step(node, new RaftInput.Applied(application.appliedThroughIndex()));
            node.applied = Math.max(node.applied, application.appliedThroughIndex());
            resolveWritesThrough(node, node.applied);
        }

        for (var readState : application.readStates()) {
            var read = node.reads.get(readState.context());
            if (read == null) {
                continue;
            }
            if (readState.index() == 0) {
                node.reads.remove(read.id());
                node.readIndexes.remove(read.id());
                read.client().pending = null;
            } else {
                node.readIndexes.put(read.id(), readState.index());
            }
        }
        serveReads(node);
    }

    private void resolveWritesThrough(Node node, long index) {
        var it = node.writes.values().iterator();
        while (it.hasNext()) {
            var write = it.next();
            if (write.index() <= index) {
                it.remove();
                abandon(write);
            }
        }
    }

    private void serveReads(Node node) {
        var it = node.reads.values().iterator();
        while (it.hasNext()) {
            var read = it.next();
            Long readIndex = node.readIndexes.get(read.id());
            if (readIndex != null && node.applied >= readIndex) {
                it.remove();
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
        node.log.snapshot().ifPresent(metadata -> send(
            node.id,
            peer,
            new RaftMessage.InstallSnapshot(node.raft.term(), node.id, metadata),
            node.snapshotState
        ));
    }

    private void send(String from, String to, RaftMessage message, Map<String, String> snapshot) {
        if (random.nextDouble() < settings.dropRate()) {
            return;
        }
        network.add(new Envelope(now + 1 + random.nextInt(settings.maxDelay()), sequence++, from, to, message, snapshot));
        if (random.nextDouble() < settings.duplicateRate()) {
            network.add(new Envelope(now + 1 + random.nextInt(settings.maxDelay()), sequence++, from, to, message, snapshot));
        }
    }

    private void observe(Node node) {
        var raft = node.raft;
        if (raft.isLeader()) {
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
        long index,
        long term
    ) implements Pending {}

    private record PendingRead(Client client, Node node, String key, long call, long issuedAt, long id) implements Pending {}

    private static final class Client {
        private final int id;
        private int counter;
        private Pending pending;

        private Client(int id) {
            this.id = id;
        }
    }

    private static final class Node {
        private final String id;
        private final InMemoryRaftLog log;
        private final Map<Long, PendingWrite> writes = new HashMap<>();
        private final Map<Long, PendingRead> reads = new LinkedHashMap<>();
        private final Map<Long, Long> readIndexes = new HashMap<>();
        private Map<String, String> snapshotState = Map.of();
        private Map<String, String> incomingSnapshot;
        private RaftNode raft;
        private Map<String, String> kv;
        private long applied;
        private long lastDataIndex;
        private long lastCommit;
        private boolean up;
        private long downUntil;

        private Node(String id, InMemoryRaftLog log) {
            this.id = Objects.requireNonNull(id);
            this.log = log;
        }
    }
}
