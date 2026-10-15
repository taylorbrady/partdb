package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftEffects.ProposalResult;
import io.partdb.raft.RaftEffects.ReadResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.random.RandomGenerator;

final class TestNode {

    static final RaftOptions OPTIONS = RaftOptions.defaults();
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
        List<RaftLogEntry> persisted,
        List<RaftLogEntry> committed,
        List<ProposalResult> proposals,
        List<ReadResult> reads,
        Set<String> snapshotRequests,
        List<SnapshotMetadata> snapshots,
        List<RaftHardState> hardStates
    ) {
        <T extends RaftMessage> List<T> sent(Class<T> type) {
            return messages.stream().map(Envelope::message).filter(type::isInstance).map(type::cast).toList();
        }

        <T extends RaftMessage> List<T> sentTo(String peer, Class<T> type) {
            return messages.stream()
                .filter(envelope -> envelope.to().equals(peer))
                .map(Envelope::message)
                .filter(type::isInstance)
                .map(type::cast)
                .toList();
        }

        <T extends RaftMessage> T single(Class<T> type) {
            var matching = sent(type);
            if (matching.size() != 1) {
                throw new AssertionError("expected one " + type.getSimpleName() + " but found " + matching);
            }
            return matching.getFirst();
        }

        <T extends RaftMessage> T singleTo(String peer, Class<T> type) {
            var matching = sentTo(peer, type);
            if (matching.size() != 1) {
                throw new AssertionError("expected one " + type.getSimpleName() + " to " + peer + " but found " + matching);
            }
            return matching.getFirst();
        }
    }

    final String id;
    final InMemoryRaftLog log;
    Raft raft;

    private TestNode(String id, InMemoryRaftLog log, Raft raft) {
        this.id = id;
        this.log = log;
        this.raft = raft;
    }

    static TestNode create(String id, String... voters) {
        return create(id, RaftMembership.voters(voters));
    }

    static TestNode create(String id, RaftMembership membership) {
        return create(id, membership, OPTIONS);
    }

    static TestNode create(String id, RaftMembership membership, RaftOptions options) {
        var log = new InMemoryRaftLog();
        return new TestNode(id, log, Raft.recover(id, membership, options, log, RaftHardState.INITIAL, 0, NO_JITTER));
    }

    static TestNode restore(String id, RaftMembership membership, InMemoryRaftLog log, long applied) {
        return new TestNode(id, log, Raft.recover(id, membership, OPTIONS, log, log.hardState(), applied, NO_JITTER));
    }

    Output step(RaftInput... inputs) {
        for (RaftInput input : inputs) {
            raft.step(input);
        }
        return drain();
    }

    Output receive(String from, RaftMessage message) {
        return step(new RaftInput.Receive(from, message));
    }

    Output tick(int ticks) {
        var inputs = new RaftInput[ticks];
        java.util.Arrays.fill(inputs, new RaftInput.Tick());
        return step(inputs);
    }

    Output propose(long id, String command) {
        return step(new RaftInput.Propose(id, Bytes.utf8(command)));
    }

    Output drain() {
        var messages = new ArrayList<Envelope>();
        var persisted = new ArrayList<RaftLogEntry>();
        var committed = new ArrayList<RaftLogEntry>();
        var proposals = new ArrayList<ProposalResult>();
        var reads = new ArrayList<ReadResult>();
        var snapshotRequests = new TreeSet<String>();
        var snapshots = new ArrayList<SnapshotMetadata>();
        var hardStates = new ArrayList<RaftHardState>();
        for (var effects = raft.ready(); !effects.isEmpty(); effects = raft.ready()) {
            log.persist(effects);
            effects.hardState().ifPresent(hardStates::add);
            effects.snapshot().ifPresent(snapshots::add);
            messages.addAll(effects.messages());
            persisted.addAll(effects.entries());
            committed.addAll(effects.committed());
            proposals.addAll(effects.proposals());
            reads.addAll(effects.reads());
            snapshotRequests.addAll(effects.snapshotRequests());
            effects.persisted().ifPresent(raft::step);
        }
        return new Output(messages, persisted, committed, proposals, reads, snapshotRequests, snapshots, hardStates);
    }

    Output campaign() {
        return tick(OPTIONS.electionTicks());
    }

    Output elect(String... voters) {
        campaign();
        for (String voter : voters) {
            receive(voter, new RaftMessage.PreVoteResponse(raft.term() + 1, true));
            if (raft.role() == RaftRole.CANDIDATE) {
                break;
            }
        }
        Output output = null;
        for (String voter : voters) {
            output = receive(voter, new RaftMessage.RequestVoteResponse(raft.term(), true));
            if (raft.role() == RaftRole.LEADER) {
                break;
            }
        }
        if (raft.role() != RaftRole.LEADER) {
            throw new AssertionError(id + " did not become leader");
        }
        return output;
    }

    Output electAndCommit(String... voters) {
        elect(voters);
        Output output = null;
        for (String voter : voters) {
            output = receive(voter, new RaftMessage.AppendAccepted(raft.term(), raft.lastIndex()));
        }
        if (raft.commitIndex() != raft.lastIndex()) {
            throw new AssertionError(id + " did not commit its no-op");
        }
        return output;
    }

    Output follow(String leader, long term) {
        return receive(leader, new RaftMessage.Heartbeat(term, 0, 0));
    }

    Optional<String> leader() {
        return raft.leaderId();
    }
}
