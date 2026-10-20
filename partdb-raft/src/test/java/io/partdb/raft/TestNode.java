package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.random.RandomGenerator;

import static io.partdb.raft.Ids.id;

final class TestNode {

    static final Options OPTIONS = Options.defaults();
    static final RandomGenerator NO_JITTER = jitter(0);

    static RandomGenerator jitter(int ticks) {
        return new RandomGenerator() {
            @Override
            public long nextLong() {
                return ticks;
            }

            @Override
            public int nextInt(int bound) {
                return ticks % bound;
            }
        };
    }

    record Output(
        List<Envelope> messages,
        List<LogEntry> persisted,
        List<LogEntry> committed,
        List<Outcome> outcomes,
        Set<NodeId> snapshotRequests,
        List<SnapshotMetadata> snapshots,
        List<HardState> hardStates
    ) {
        <T extends Message> List<T> sent(Class<T> type) {
            return messages.stream().map(Envelope::message).filter(type::isInstance).map(type::cast).toList();
        }

        <T extends Message> List<T> sentTo(String peer, Class<T> type) {
            return messages.stream()
                .filter(envelope -> envelope.to().equals(id(peer)))
                .map(Envelope::message)
                .filter(type::isInstance)
                .map(type::cast)
                .toList();
        }

        <T extends Message> T single(Class<T> type) {
            var matching = sent(type);
            if (matching.size() != 1) {
                throw new AssertionError("expected one " + type.getSimpleName() + " but found " + matching);
            }
            return matching.getFirst();
        }

        <T extends Message> T singleTo(String peer, Class<T> type) {
            var matching = sentTo(peer, type);
            if (matching.size() != 1) {
                throw new AssertionError("expected one " + type.getSimpleName() + " to " + peer + " but found " + matching);
            }
            return matching.getFirst();
        }
    }

    final NodeId id;
    final InMemoryRaftLog log;
    Raft raft;

    private TestNode(NodeId id, InMemoryRaftLog log, Raft raft) {
        this.id = id;
        this.log = log;
        this.raft = raft;
    }

    static TestNode create(String id, String... voters) {
        return create(id, Ids.voters(voters));
    }

    static TestNode create(String id, Membership membership) {
        return create(id, membership, OPTIONS);
    }

    static TestNode create(String id, Membership membership, Options options) {
        var log = new InMemoryRaftLog();
        return new TestNode(id(id), log, Raft.recover(id(id), membership, options, log, HardState.INITIAL, 0, NO_JITTER));
    }

    static TestNode restore(String id, Membership membership, InMemoryRaftLog log, long applied) {
        return new TestNode(id(id), log, Raft.recover(id(id), membership, OPTIONS, log, log.hardState(), applied, NO_JITTER));
    }

    Output step(Input... inputs) {
        for (Input input : inputs) {
            raft.step(input);
        }
        return drain();
    }

    Output receive(String from, Message message) {
        return step(new Input.Receive(id(from), message));
    }

    Output installSnapshot(String from, long term, SnapshotMetadata snapshot) {
        return step(new Input.InstallSnapshot(id(from), term, snapshot));
    }

    Output tick(int ticks) {
        var inputs = new Input[ticks];
        Arrays.fill(inputs, new Input.Tick());
        return step(inputs);
    }

    Output propose(long id, String command) {
        return step(new Input.Propose(id, Bytes.utf8(command)));
    }

    Output drain() {
        var messages = new ArrayList<Envelope>();
        var persisted = new ArrayList<LogEntry>();
        var committed = new ArrayList<LogEntry>();
        var outcomes = new ArrayList<Outcome>();
        var snapshotRequests = new TreeSet<NodeId>();
        var snapshots = new ArrayList<SnapshotMetadata>();
        var hardStates = new ArrayList<HardState>();
        for (var ready = raft.ready(); !ready.isEmpty(); ready = raft.ready()) {
            messages.addAll(ready.messages());
            log.persist(ready);
            ready.hardState().ifPresent(hardStates::add);
            ready.snapshot().ifPresent(snapshots::add);
            messages.addAll(ready.persistedMessages());
            persisted.addAll(ready.entries());
            committed.addAll(ready.committed());
            outcomes.addAll(ready.outcomes());
            snapshotRequests.addAll(ready.snapshotRequests());
            ready.persisted().ifPresent(raft::step);
        }
        return new Output(messages, persisted, committed, outcomes, snapshotRequests, snapshots, hardStates);
    }

    Output campaign() {
        return tick(OPTIONS.electionTicks());
    }

    Output elect(String... voters) {
        campaign();
        for (String voter : voters) {
            receive(voter, new Message.PreVoteResponse(raft.term() + 1, true));
            if (raft.role() == Role.CANDIDATE) {
                break;
            }
        }
        Output output = null;
        for (String voter : voters) {
            output = receive(voter, new Message.RequestVoteResponse(raft.term(), true));
            if (raft.role() == Role.LEADER) {
                break;
            }
        }
        if (raft.role() != Role.LEADER) {
            throw new AssertionError(id + " did not become leader");
        }
        return output;
    }

    Output electAndCommit(String... voters) {
        elect(voters);
        Output output = null;
        for (String voter : voters) {
            output = receive(voter, new Message.AppendAccepted(raft.term(), raft.lastIndex()));
        }
        if (raft.commitIndex() != raft.lastIndex()) {
            throw new AssertionError(id + " did not commit its no-op");
        }
        return output;
    }

    Output follow(String leader, long term) {
        return receive(leader, new Message.Heartbeat(term, 0, 0));
    }

    Optional<String> leader() {
        return raft.leaderId().map(NodeId::value);
    }
}
