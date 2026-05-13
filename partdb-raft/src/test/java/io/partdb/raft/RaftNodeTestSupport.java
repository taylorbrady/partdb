package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

abstract class RaftNodeTestSupport {

    static final RaftOptions CONFIG = RaftOptions.defaults();
    static final int DETERMINISTIC_JITTER = 0;

    static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    RaftNode createRaft(String id, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryRaftLog(configuration);
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .build();
    }

    RaftNode createRaft(String id, RaftMembership configuration, RaftOptions options) {
        var storage = new InMemoryRaftLog(configuration);
        return RaftNode.builder(id, configuration, options, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .build();
    }

    RaftNode createRaftWithLog(String id, List<RaftLogEntry> entries, RaftHardState hardState, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryRaftLog(configuration);
        storage.append(null, entries);
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .restoredFrom(hardState, 0)
            .build();
    }

    RaftNode createRaftWithSnapshot(String id, RaftHardState hardState, long snapIndex, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryRaftLog(configuration);
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .restoredFrom(hardState, snapIndex)
            .build();
    }

    RaftNode createRaftWithCompactedSnapshot(String id, long snapIndex, long snapTerm, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryRaftLog(configuration);
        for (int i = 1; i <= snapIndex; i++) {
            storage.append(null, List.of(new RaftLogEntry.Data(i, snapTerm, Bytes.EMPTY)));
        }
        storage.saveSnapshot(new RaftSnapshot(snapIndex, snapTerm, configuration, Bytes.EMPTY));
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .restoredFrom(RaftHardState.INITIAL, snapIndex)
            .build();
    }

    RaftEffects tickUntilTimeout(RaftNode raft) {
        RaftEffects ready = null;
        for (int i = 0; i < CONFIG.electionTimeoutMin(); i++) {
            ready = raft.step(new RaftInput.Tick());
        }
        return ready;
    }

    RaftEffects grantPreVotes(RaftNode raft, List<String> peers) {
        long term = raft.term();
        RaftEffects ready = null;
        for (String peer : peers) {
            ready = raft.step(new RaftInput.MessageReceived(peer, new RaftMessage.PreVoteResponse(term, true)));
            if (raft.role() == RaftRole.CANDIDATE) break;
        }
        return ready;
    }

    RaftEffects grantVotes(RaftNode raft, List<String> peers) {
        long term = raft.term();
        RaftEffects ready = null;
        for (String peer : peers) {
            ready = raft.step(new RaftInput.MessageReceived(peer, new RaftMessage.RequestVoteResponse(term, true)));
            if (raft.isLeader()) break;
        }
        return ready;
    }

    RaftEffects tickUntilCandidate(RaftNode raft, List<String> peers) {
        tickUntilTimeout(raft);
        return grantPreVotes(raft, peers);
    }

    RaftEffects becomeLeader(RaftNode raft, List<String> peers) {
        tickUntilCandidate(raft, peers);
        return grantVotes(raft, peers);
    }

    <T extends RaftMessage.Response> Optional<T> findResponse(RaftEffects ready, Class<T> type) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    <T extends RaftMessage.Request> Optional<T> findRequest(RaftEffects ready, Class<T> type) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    <T extends RaftLogEntry> Optional<T> findEntry(RaftEffects ready, Class<T> type) {
        return ready.persistence().entries().stream()
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    List<RaftMessage.AppendEntries> getAppendEntries(RaftEffects ready) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(RaftMessage.AppendEntries.class::isInstance)
            .map(RaftMessage.AppendEntries.class::cast)
            .toList();
    }

    List<RaftMessage.PreVote> getPreVotes(RaftEffects ready) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(RaftMessage.PreVote.class::isInstance)
            .map(RaftMessage.PreVote.class::cast)
            .toList();
    }

    List<RaftMessage.RequestVote> getRequestVotes(RaftEffects ready) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(RaftMessage.RequestVote.class::isInstance)
            .map(RaftMessage.RequestVote.class::cast)
            .toList();
    }

    RaftEffects tickHeartbeat(RaftNode raft) {
        RaftEffects ready = null;
        for (int i = 0; i < CONFIG.heartbeatInterval(); i++) {
            ready = raft.step(new RaftInput.Tick());
        }
        return ready;
    }
}
