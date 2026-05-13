package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {

    private static final RaftOptions CONFIG = RaftOptions.defaults();
    private static final int DETERMINISTIC_JITTER = 0;

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    private RaftNode createRaft(String id, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryStorage(configuration);
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .build();
    }

    private RaftNode createRaft(String id, RaftMembership configuration, RaftOptions options) {
        var storage = new InMemoryStorage(configuration);
        return RaftNode.builder(id, configuration, options, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .build();
    }

    private RaftNode createRaftWithLog(String id, List<RaftLogEntry> entries, RaftHardState hardState, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryStorage(configuration);
        storage.append(null, entries);
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .restoredFrom(hardState, 0)
            .build();
    }

    private RaftNode createRaftWithSnapshot(String id, RaftHardState hardState, long snapIndex, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryStorage(configuration);
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .restoredFrom(hardState, snapIndex)
            .build();
    }

    private RaftNode createRaftWithCompactedSnapshot(String id, long snapIndex, long snapTerm, String... allVoters) {
        var configuration = RaftMembership.voters(allVoters);
        var storage = new InMemoryStorage(configuration);
        for (int i = 1; i <= snapIndex; i++) {
            storage.append(null, List.of(new RaftLogEntry.Data(i, snapTerm, Bytes.EMPTY)));
        }
        storage.saveSnapshot(new RaftSnapshot(snapIndex, snapTerm, configuration, Bytes.EMPTY));
        return RaftNode.builder(id, configuration, CONFIG, storage)
            .electionJitter(_ -> DETERMINISTIC_JITTER)
            .restoredFrom(RaftHardState.INITIAL, snapIndex)
            .build();
    }

    private RaftEffects tickUntilTimeout(RaftNode raft) {
        RaftEffects ready = null;
        for (int i = 0; i < CONFIG.electionTimeoutMin(); i++) {
            ready = raft.step(new RaftInput.Tick());
        }
        return ready;
    }

    private RaftEffects grantPreVotes(RaftNode raft, List<String> peers) {
        long term = raft.term();
        RaftEffects ready = null;
        for (String peer : peers) {
            ready = raft.step(new RaftInput.MessageReceived(peer, new RaftMessage.PreVoteResponse(term, true)));
            if (raft.role() == RaftRole.CANDIDATE) break;
        }
        return ready;
    }

    private RaftEffects grantVotes(RaftNode raft, List<String> peers) {
        long term = raft.term();
        RaftEffects ready = null;
        for (String peer : peers) {
            ready = raft.step(new RaftInput.MessageReceived(peer, new RaftMessage.RequestVoteResponse(term, true)));
            if (raft.isLeader()) break;
        }
        return ready;
    }

    private RaftEffects tickUntilCandidate(RaftNode raft, List<String> peers) {
        tickUntilTimeout(raft);
        return grantPreVotes(raft, peers);
    }

    private RaftEffects becomeLeader(RaftNode raft, List<String> peers) {
        tickUntilCandidate(raft, peers);
        return grantVotes(raft, peers);
    }

    private <T extends RaftMessage.Response> Optional<T> findResponse(RaftEffects ready, Class<T> type) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    private <T extends RaftMessage.Request> Optional<T> findRequest(RaftEffects ready, Class<T> type) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    private <T extends RaftLogEntry> Optional<T> findEntry(RaftEffects ready, Class<T> type) {
        return ready.persistence().entries().stream()
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    private List<RaftMessage.AppendEntries> getAppendEntries(RaftEffects ready) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(RaftMessage.AppendEntries.class::isInstance)
            .map(RaftMessage.AppendEntries.class::cast)
            .toList();
    }

    private List<RaftMessage.PreVote> getPreVotes(RaftEffects ready) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(RaftMessage.PreVote.class::isInstance)
            .map(RaftMessage.PreVote.class::cast)
            .toList();
    }

    private List<RaftMessage.RequestVote> getRequestVotes(RaftEffects ready) {
        return ready.messages().stream()
            .map(RaftEffects.Outbound::message)
            .filter(RaftMessage.RequestVote.class::isInstance)
            .map(RaftMessage.RequestVote.class::cast)
            .toList();
    }

    private RaftEffects tickHeartbeat(RaftNode raft) {
        RaftEffects ready = null;
        for (int i = 0; i < CONFIG.heartbeatInterval(); i++) {
            ready = raft.step(new RaftInput.Tick());
        }
        return ready;
    }

    @Nested
    class InitialState {
        @Test
        void startsAsFollower() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void startsAtTermZero() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());
        }

        @Test
        void startsWithNoLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertTrue(raft.leaderId().isEmpty());
        }

        @Test
        void startsWithCommitIndexZero() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.commitIndex());
        }
    }

    @Nested
    class ElectionTimeout {
        @Test
        void followerStartsElectionAfterTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftInput.Tick());
            }
            assertEquals(RaftRole.FOLLOWER, raft.role());

            raft.step(new RaftInput.Tick());
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void candidateRestartsElectionAfterTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(1, raft.term());

            tickUntilTimeout(raft);
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(2, raft.term());
        }

        @Test
        void leaderDoesNotTimeoutElection() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());
            long term = raft.term();

            for (int i = 0; i < 50; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertTrue(raft.isLeader());
            assertEquals(term, raft.term());
        }

        @Test
        void electionTimeoutIsRandomized() {
            var counter = new AtomicInteger(0);
            var configuration = RaftMembership.voters("n1", "n2");
            var storage = new InMemoryStorage(configuration);
            var raft = RaftNode.builder("n1", configuration, RaftOptions.defaults(), storage)
                .electionJitter(_ -> counter.incrementAndGet())
                .build();

            for (int i = 0; i < 50; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertTrue(counter.get() > 1);
        }
    }

    @Nested
    class Voting {
        @Test
        void grantsVoteToFirstCandidateWithUpToDateLog() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response.voteGranted());
        }

        @Test
        void rejectsVoteIfAlreadyVotedForAnother() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request1 = new RaftMessage.RequestVote(1, "n2", 0, 0);
            raft.step(new RaftInput.MessageReceived("n2", request1));

            var request2 = new RaftMessage.RequestVote(1, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request2));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void rejectsVoteIfCandidateLogBehind() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            var request = new RaftMessage.RequestVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void rejectsVoteFromLowerTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            var request = new RaftMessage.RequestVote(3, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void votingPersistsHardState() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            assertNotNull(ready.persistence().hardState().orElse(null));
            assertEquals(1, ready.persistence().hardState().orElseThrow().term());
            assertEquals("n2", ready.persistence().hardState().orElseThrow().votedFor());
        }
    }

    @Nested
    class LeaderElection {
        @Test
        void candidateBecomesLeaderWithMajority() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.RequestVoteResponse(1, true)));

            assertTrue(raft.isLeader());
        }

        @Test
        void candidateIgnoresPositiveVoteFromNonMember() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n4", new RaftMessage.RequestVoteResponse(1, true)));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertTrue(raft.isLeader());
        }

        @Test
        void singleNodeBecomesLeaderImmediately() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);

            assertTrue(raft.isLeader());
            assertEquals(1, raft.term());
        }

        @Test
        void leaderAppendsNoOpOnElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = becomeLeader(raft, List.of("n2", "n3"));

            assertFalse(ready.persistence().entries().isEmpty());
            var noOp = ready.persistence().entries().stream()
                .filter(e -> e instanceof RaftLogEntry.NoOp)
                .findFirst();
            assertTrue(noOp.isPresent());
        }

        @Test
        void leaderSendsAppendEntriesToPeersOnElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = becomeLeader(raft, List.of("n2", "n3"));

            long appendCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .count();
            assertEquals(2, appendCount);
        }
    }

    @Nested
    class TermChanges {
        @Test
        void stepDownToFollowerOnHigherTermMessage() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));
            assertTrue(raft.isLeader());

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals(5, raft.term());
        }

        @Test
        void rejectMessagesFromLowerTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            var lowerTermAppend = new RaftMessage.AppendEntries(3, "n3", 0, 0, List.of(), 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", lowerTermAppend));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
            assertEquals(5, response.term());
        }

        @Test
        void updateTermOnHigherTermResponse() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));
            assertEquals(1, raft.term());

            var higherTermResponse = new RaftMessage.AppendEntriesResponse(5, false, 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermResponse));

            assertEquals(5, raft.term());
            assertEquals(RaftRole.FOLLOWER, raft.role());
        }
    }

    @Nested
    class AppendEntries {
        @Test
        void followerAcceptsMatchingAppendEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(1, response.matchIndex());
        }

        @Test
        void followerRejectsMismatchedPrevLogTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 1, 1, List.of(), 0
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
        }

        @Test
        void followerAdvancesCommitIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                1
            );
            raft.step(new RaftInput.MessageReceived("n2", request));

            assertEquals(1, raft.commitIndex());
        }

        @Test
        void followerResetsElectionTimerOnAppendEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftInput.Tick());
            }

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void followerPersistsNewEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            assertEquals(1, ready.persistence().entries().size());
        }
    }

    @Nested
    class LogReplication {
        @Test
        void leaderBroadcastsAppendEntriesOnPropose() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            var ready = raft.step(new RaftInput.EntryProposed(bytes("hello")));

            long appendCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .count();
            assertEquals(2, appendCount);
        }

        @Test
        void leaderAdvancesCommitOnMajorityMatch() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            raft.step(new RaftInput.EntryProposed(bytes("hello")));

            var response = new RaftMessage.AppendEntriesResponse(1, true, 2);
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderDecrementsNextIndexOnReject() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", reject));

            var retryAppend = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .filter(m -> m.to().equals("n2"))
                .map(m -> (RaftMessage.AppendEntries) m.message())
                .findFirst();

            assertTrue(retryAppend.isPresent());
        }

        @Test
        void singleNodeLeaderCommitsImmediately() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);

            raft.step(new RaftInput.EntryProposed(bytes("hello")));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void nonLeaderIgnoresPropose() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.EntryProposed(bytes("hello")));

            assertTrue(ready.persistence().entries().isEmpty());
            assertTrue(ready.messages().isEmpty());
        }
    }

    @Nested
    class Heartbeats {
        @Test
        void leaderSendsHeartbeatsAtInterval() {
            var config = new RaftOptions(10, 20, 3, 100);
            var raft = createRaft("n1", RaftMembership.voters("n1", "n2", "n3"), config);
            becomeLeader(raft, List.of("n2"));

            for (int i = 0; i < 2; i++) {
                raft.step(new RaftInput.Tick());
            }

            var ready = raft.step(new RaftInput.Tick());

            long heartbeatCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .count();
            assertEquals(2, heartbeatCount);
        }

        @Test
        void heartbeatContainsCommitIndex() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);

            raft.step(new RaftInput.EntryProposed(bytes("data")));

            long commitIndex = raft.commitIndex();
            assertTrue(commitIndex > 0);
        }
    }

    @Nested
    class Snapshots {
        @Test
        void followerInstallsSnapshotFromLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 10, 1, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", snapshot));

            assertNotNull(ready.persistence().incomingSnapshot().orElse(null));
            assertEquals(10, ready.persistence().incomingSnapshot().orElseThrow().index());
        }

        @Test
        void followerRejectsSnapshotBelowCommitIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var entries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                1
            );
            raft.step(new RaftInput.MessageReceived("n2", entries));
            assertEquals(1, raft.commitIndex());

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 1, 1, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", snapshot));

            assertNull(ready.persistence().incomingSnapshot().orElse(null));
        }

        @Test
        void leaderSendsSnapshotWhenFollowerBehindCompactedLog() {
            var raft = createRaftWithCompactedSnapshot("n1", 1, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", reject));

            assertNotNull(ready.snapshotNeeded().orElse(null));
            assertEquals("n2", ready.snapshotNeeded().orElse(null).peer());
        }

        @Test
        void leaderUpdatesIndicesOnInstallSnapshotResponse() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var response = new RaftMessage.InstallSnapshotResponse(raft.term());
            raft.step(new RaftInput.MessageReceived("n2", response));

            var ready = tickHeartbeat(raft);

            var appendToN2 = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(m -> (RaftMessage.AppendEntries) m.message())
                .findFirst();

            assertTrue(appendToN2.isPresent());
            assertEquals(5, appendToN2.get().prevLogIndex());
        }

        @Test
        void leaderIgnoresInstallSnapshotResponseFromOldTerm() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var snapshotReady = raft.step(new RaftInput.MessageReceived("n2", reject));
            assertNotNull(snapshotReady.snapshotNeeded().orElse(null));
            assertEquals("n2", snapshotReady.snapshotNeeded().orElse(null).peer());

            var oldTermResponse = new RaftMessage.InstallSnapshotResponse(0);
            raft.step(new RaftInput.MessageReceived("n2", oldTermResponse));

            var ready = tickHeartbeat(raft);

            var snapshotToN2 = ready.snapshotNeeded().orElse(null);
            assertNotNull(snapshotToN2);
            assertEquals("n2", snapshotToN2.peer());
        }

        @Test
        void leaderIgnoresInstallSnapshotResponseWhenNotLeader() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var higherTermMsg = new RaftMessage.AppendEntries(10, "n3", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n3", higherTermMsg));
            assertEquals(RaftRole.FOLLOWER, raft.role());

            var response = new RaftMessage.InstallSnapshotResponse(1);
            var ready = raft.step(new RaftInput.MessageReceived("n2", response));

            assertTrue(ready.messages().isEmpty());
        }

        @Test
        void installSnapshotFromLowerTermIsRejected() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));
            assertEquals(5, raft.term());

            var snapshot = new RaftMessage.InstallSnapshot(
                3, "n3", 10, 3, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            var ready = raft.step(new RaftInput.MessageReceived("n3", snapshot));

            assertNull(ready.persistence().incomingSnapshot().orElse(null));

            var response = findResponse(ready, RaftMessage.InstallSnapshotResponse.class).orElseThrow();
            assertEquals(5, response.term());
        }

        @Test
        void installSnapshotResetsLeaderContactTicks() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 5; i++) {
                raft.step(new RaftInput.Tick());
            }

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 10, 1, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            raft.step(new RaftInput.MessageReceived("n2", snapshot));

            var preVote = new RaftMessage.PreVote(2, "n3", 10, 1);
            var ready = raft.step(new RaftInput.MessageReceived("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }
    }

    @Nested
    class CommitSafety {
        @Test
        void leaderDoesNotCommitEntriesFromPreviousTerm() {
            var oldEntry = new RaftLogEntry.Data(1, 1, bytes("old"));
            var raft = createRaftWithLog("n1", List.of(oldEntry), new RaftHardState(1, null, 0), "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(2, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(2, true)));
            assertTrue(raft.isLeader());

            var response = new RaftMessage.AppendEntriesResponse(2, true, 1);
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(0, raft.commitIndex());
        }

        @Test
        void leaderCommitsCurrentTermEntryImplicitlyCommittingPrevious() {
            var oldEntry = new RaftLogEntry.Data(1, 1, bytes("old"));
            var raft = createRaftWithLog("n1", List.of(oldEntry), new RaftHardState(1, null, 0), "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(2, true)));
            assertTrue(raft.isLeader());

            var response = new RaftMessage.AppendEntriesResponse(2, true, 2);
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderRequiresMajorityForCurrentTermEntry() {
            var oldEntry = new RaftLogEntry.Data(1, 1, bytes("old"));
            var raft = createRaftWithLog("n1", List.of(oldEntry), new RaftHardState(1, null, 0), "n1", "n2", "n3", "n4", "n5");

            tickUntilCandidate(raft, List.of("n2", "n3", "n4", "n5"));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(2, true)));
            raft.step(new RaftInput.MessageReceived("n3", new RaftMessage.RequestVoteResponse(2, true)));
            assertTrue(raft.isLeader());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(2, true, 2)));
            assertEquals(0, raft.commitIndex());

            raft.step(new RaftInput.MessageReceived("n3", new RaftMessage.AppendEntriesResponse(2, true, 2)));
            assertEquals(2, raft.commitIndex());
        }
    }

    @Nested
    class PersistenceRecovery {
        @Test
        void restoresTermFromHardState() {
            var raft = createRaftWithSnapshot("n1", new RaftHardState(5, null, 0), 0, "n1", "n2", "n3");

            assertEquals(5, raft.term());
        }

        @Test
        void restoresVotedForFromHardState() {
            var raft = createRaftWithSnapshot("n1", new RaftHardState(3, "n2", 0), 0, "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(3, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void restoresLogEntries() {
            List<RaftLogEntry> entries = List.of(
                new RaftLogEntry.Data(1, 1, bytes("first")),
                new RaftLogEntry.Data(2, 1, bytes("second"))
            );
            var raft = createRaftWithLog("n1", entries, new RaftHardState(1, null, 0), "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            var ready = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.RequestVoteResponse(2, true)));

            var append = ready.messages().stream()
                .map(RaftEffects.Outbound::message)
                .filter(m -> m instanceof RaftMessage.AppendEntries)
                .map(m -> (RaftMessage.AppendEntries) m)
                .findFirst()
                .orElse(null);

            assertNotNull(append);
            assertEquals(2, append.prevLogIndex());
            assertEquals(1, append.prevLogTerm());
        }

        @Test
        void restoresCommitIndexFromSnapshot() {
            var raft = createRaftWithSnapshot("n1", new RaftHardState(2, null, 0), 5, "n1", "n2", "n3");

            assertEquals(5, raft.commitIndex());
        }

        @Test
        void restoredNodeCanBecomeLeader() {
            List<RaftLogEntry> entries = List.of(new RaftLogEntry.Data(1, 1, bytes("data")));
            var raft = createRaftWithLog("n1", entries, new RaftHardState(1, null, 0), "n1");

            tickUntilTimeout(raft);

            assertTrue(raft.isLeader());
            assertEquals(2, raft.term());

            raft.step(new RaftInput.EntryProposed(bytes("new-data")));
            assertEquals(3, raft.commitIndex());
        }

        @Test
        void restoredNodeRejectsOldTermMessages() {
            var raft = createRaftWithSnapshot("n1", new RaftHardState(10, null, 0), 0, "n1", "n2", "n3");

            var oldAppend = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", oldAppend));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
            assertEquals(10, response.term());
        }
    }

    @Nested
    class LogConflictResolution {
        @Test
        void followerTruncatesConflictingEntries() {
            List<RaftLogEntry> entries = List.of(
                new RaftLogEntry.Data(1, 1, bytes("a")),
                new RaftLogEntry.Data(2, 1, bytes("b")),
                new RaftLogEntry.Data(3, 1, bytes("c"))
            );
            var raft = createRaftWithLog("n1", entries, new RaftHardState(1, null, 0), "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                2, "n2", 1, 1,
                List.of(
                    new RaftLogEntry.Data(2, 2, bytes("new-b")),
                    new RaftLogEntry.Data(3, 2, bytes("new-c"))
                ),
                0
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            assertEquals(2, ready.persistence().entries().size());
            assertEquals(2, ready.persistence().entries().get(0).term());
            assertEquals(2, ready.persistence().entries().get(1).term());

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(3, response.matchIndex());
        }

        @Test
        void followerAcceptsEntriesAfterConflictResolution() {
            List<RaftLogEntry> entries = List.of(
                new RaftLogEntry.Data(1, 1, bytes("a")),
                new RaftLogEntry.Data(2, 1, bytes("b"))
            );
            var raft = createRaftWithLog("n1", entries, new RaftHardState(1, null, 0), "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                2, "n2", 1, 1,
                List.of(
                    new RaftLogEntry.Data(2, 2, bytes("new-b")),
                    new RaftLogEntry.Data(3, 2, bytes("new-c")),
                    new RaftLogEntry.Data(4, 2, bytes("new-d"))
                ),
                0
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            assertEquals(3, ready.persistence().entries().size());

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(4, response.matchIndex());
        }

        @Test
        void followerRejectsIfPrevLogDoesNotMatch() {
            List<RaftLogEntry> entries = List.of(
                new RaftLogEntry.Data(1, 1, bytes("a")),
                new RaftLogEntry.Data(2, 1, bytes("b"))
            );
            var raft = createRaftWithLog("n1", entries, new RaftHardState(1, null, 0), "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                2, "n2", 2, 2,
                List.of(new RaftLogEntry.Data(3, 2, bytes("c"))),
                0
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            assertTrue(ready.persistence().entries().isEmpty());

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
        }

        @Test
        void leaderRetriesWithLowerNextIndexOnReject() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", reject));

            var retry = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .map(m -> (RaftMessage.AppendEntries) m.message())
                .findFirst()
                .orElse(null);

            assertNotNull(retry);
            assertEquals(0, retry.prevLogIndex());

            var accept = new RaftMessage.AppendEntriesResponse(1, true, 1);
            raft.step(new RaftInput.MessageReceived("n2", accept));

            assertEquals(1, raft.commitIndex());
        }
    }

    @Nested
    class DuplicateRpcHandling {
        @Test
        void duplicateAppendEntriesIsIdempotent() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );

            var ready1 = raft.step(new RaftInput.MessageReceived("n2", appendEntries));
            assertEquals(1, ready1.persistence().entries().size());
            var response1 = findResponse(ready1, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response1.success());
            assertEquals(1, response1.matchIndex());

            var ready2 = raft.step(new RaftInput.MessageReceived("n2", appendEntries));
            assertTrue(ready2.persistence().entries().isEmpty());
            var response2 = findResponse(ready2, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response2.success());
            assertEquals(1, response2.matchIndex());
        }

        @Test
        void duplicateRequestVoteGrantsToSameCandidate() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var requestVote = new RaftMessage.RequestVote(1, "n2", 0, 0);

            var ready1 = raft.step(new RaftInput.MessageReceived("n2", requestVote));
            var response1 = findResponse(ready1, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response1.voteGranted());

            var ready2 = raft.step(new RaftInput.MessageReceived("n2", requestVote));
            var response2 = findResponse(ready2, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response2.voteGranted());
        }

        @Test
        void duplicateAppendEntriesResponseOnlyCountsOnce() {
            var raft = createRaft("n1", "n1", "n2", "n3", "n4", "n5");
            becomeLeader(raft, List.of("n2", "n3"));

            var response = new RaftMessage.AppendEntriesResponse(1, true, 1);

            raft.step(new RaftInput.MessageReceived("n2", response));
            assertEquals(0, raft.commitIndex());

            raft.step(new RaftInput.MessageReceived("n2", response));
            assertEquals(0, raft.commitIndex());

            raft.step(new RaftInput.MessageReceived("n3", response));
            assertEquals(1, raft.commitIndex());
        }
    }

    @Nested
    class VoteSplitting {
        @Test
        void electionRestartsWhenNoMajority() {
            var raft = createRaft("n1", "n1", "n2", "n3", "n4", "n5");

            tickUntilCandidate(raft, List.of("n2", "n3", "n4", "n5"));
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(1, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            tickUntilTimeout(raft);
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(2, raft.term());
        }

        @Test
        void candidateVotesForItself() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertTrue(raft.isLeader());
        }

        @Test
        void splitVoteLeadsToNewElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(1, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, false)));
            raft.step(new RaftInput.MessageReceived("n3", new RaftMessage.RequestVoteResponse(1, false)));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            tickUntilTimeout(raft);
            assertEquals(2, raft.term());
            assertEquals(RaftRole.CANDIDATE, raft.role());
        }
    }

    @Nested
    class PreVote {
        @Test
        void followerStartsPreVoteOnTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftInput.Tick());
            }
            assertEquals(RaftRole.FOLLOWER, raft.role());

            raft.step(new RaftInput.Tick());
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
        }

        @Test
        void preVoteDoesNotIncrementTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);

            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void preVoteDoesNotPersistHardState() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.Tick());
            for (int i = 1; i < 10; i++) {
                ready = raft.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertNull(ready.persistence().hardState().orElse(null));
        }

        @Test
        void preCandidateSendsPreVoteRequests() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = tickUntilTimeout(raft);

            long preVoteCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.PreVote)
                .count();
            assertEquals(2, preVoteCount);

            var preVote = (RaftMessage.PreVote) ready.messages().get(0).message();
            assertEquals(1, preVote.term());
            assertEquals("n1", preVote.candidateId());
        }

        @Test
        void preCandidateBecomesFollowerOnAppendEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals("n2", raft.leaderId().orElseThrow());
        }

        @Test
        void preCandidateBecomesFollowerOnHigherTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals(5, raft.term());
        }

        @Test
        void preCandidateBecomesCandidateWithMajorityPreVotes() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.PreVoteResponse(0, true)));

            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(1, raft.term());
        }

        @Test
        void preVoteGrantedWhenLogUpToDateAndNoLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);

            var preVote = new RaftMessage.PreVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertTrue(response.voteGranted());
        }

        @Test
        void preVoteDeniedWhenLeaderActive() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            var preVote = new RaftMessage.PreVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void preVoteDeniedWhenLogBehind() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            tickUntilTimeout(raft);

            var preVote = new RaftMessage.PreVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void singleNodeSkipsPreVote() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);

            assertTrue(raft.isLeader());
            assertEquals(1, raft.term());
        }

        @Test
        void preCandidateRestartsPreVoteOnTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void preVoteDoesNotUpdateTermOnHigherTermPreVote() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            assertEquals(0, raft.term());

            var preVote = new RaftMessage.PreVote(10, "n2", 0, 0);
            raft.step(new RaftInput.MessageReceived("n2", preVote));

            assertEquals(0, raft.term());
            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void preVoteResponseDoesNotUpdateTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            var response = new RaftMessage.PreVoteResponse(10, false);
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(0, raft.term());
        }
    }

    @Nested
    class ReadIndexRequested {
        @Test
        void leaderConfirmsReadIndexImmediatelyForSingleNode() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            assertEquals(1, ready.application().readStates().size());
            var readState = ready.application().readStates().get(0);
            assertEquals(raft.commitIndex(), readState.index());
            assertEquals(bytes("req-1"), readState.context());
        }

        @Test
        void leaderBroadcastsHeartbeatForReadIndexConfirmation() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            assertTrue(ready.application().readStates().isEmpty());
            var appendEntries = getAppendEntries(ready);
            assertEquals(2, appendEntries.size());
        }

        @Test
        void leaderConfirmsReadIndexOnQuorumAck() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var ackReady = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            assertEquals(1, ackReady.application().readStates().size());
            assertEquals(bytes("req-1"), ackReady.application().readStates().get(0).context());
        }

        @Test
        void leaderRespondsToForwardedReadIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            long commitIndexAtRequest = raft.commitIndex();
            var readIndexMsg = new RaftMessage.ReadIndexRequested(raft.term(), bytes("req-1"));
            raft.step(new RaftInput.MessageReceived("n2", readIndexMsg));

            var ackReady = raft.step(new RaftInput.MessageReceived("n3",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            var response = findResponse(ackReady, RaftMessage.ReadIndexResponse.class);
            assertTrue(response.isPresent());
            assertEquals(commitIndexAtRequest, response.get().readIndex());
            assertEquals(bytes("req-1"), response.get().context());
        }

        @Test
        void leaderClearsPendingReadsOnBecomeFollower() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var higherTermMsg = new RaftMessage.AppendEntries(10, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));
            assertEquals(RaftRole.FOLLOWER, raft.role());

            var ackReady = raft.step(new RaftInput.MessageReceived("n3",
                new RaftMessage.AppendEntriesResponse(1, true, 1)));

            assertTrue(ackReady.application().readStates().isEmpty());
        }

        @Test
        void followerForwardsReadIndexToLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));
            assertEquals("n2", raft.leaderId().orElseThrow());

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var readIndexMsg = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .filter(m -> m.message() instanceof RaftMessage.ReadIndexRequested)
                .map(m -> (RaftMessage.ReadIndexRequested) m.message())
                .findFirst();

            assertTrue(readIndexMsg.isPresent());
            assertEquals(bytes("req-1"), readIndexMsg.get().context());
        }

        @Test
        void followerEmitsReadStateOnResponse() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 5, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(1, ready.application().readStates().size());
            assertEquals(5, ready.application().readStates().get(0).index());
            assertEquals(bytes("req-1"), ready.application().readStates().get(0).context());
        }

        @Test
        void followerDropsReadIndexWhenNoLeaderKnown() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertTrue(raft.leaderId().isEmpty());

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            assertTrue(ready.messages().isEmpty());
            assertTrue(ready.application().readStates().isEmpty());
        }

        @Test
        void readIndexResponseWithZeroIndexIndicatesFailure() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 0, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n2", response));

            assertTrue(ready.application().readStates().isEmpty());
        }

        @Test
        void nonLeaderRespondsWithZeroReadIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var readIndexMsg = new RaftMessage.ReadIndexRequested(1, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n2", readIndexMsg));

            var response = findResponse(ready, RaftMessage.ReadIndexResponse.class).orElseThrow();
            assertEquals(0, response.readIndex());
        }

        @Test
        void multipleReadIndexRequestsTrackedIndependently() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));
            raft.step(new RaftInput.ReadIndexRequested(bytes("req-2")));

            var ackReady = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            assertEquals(2, ackReady.application().readStates().size());
        }

        @Test
        void readIndexDoesNotTriggerTermChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());

            var readIndexMsg = new RaftMessage.ReadIndexRequested(10, bytes("req-1"));
            raft.step(new RaftInput.MessageReceived("n2", readIndexMsg));

            assertEquals(0, raft.term());
        }

        @Test
        void readIndexResponseDoesNotTriggerTermChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());

            var response = new RaftMessage.ReadIndexResponse(10, 5, bytes("req-1"));
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(0, raft.term());
        }

        @Test
        void followerIgnoresReadIndexResponseFromUnexpectedSender() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));
            assertEquals("n2", raft.leaderId().orElseThrow());

            var response = new RaftMessage.ReadIndexResponse(1, 5, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n3", response));

            assertTrue(ready.application().readStates().isEmpty());
        }
    }

    @Nested
    class Learners {
        private RaftNode createLearner(String id, Set<String> voters, Set<String> learners) {
            var configuration = new RaftMembership(voters, learners);
            var storage = new InMemoryStorage(configuration);
            return RaftNode.builder(id, configuration, CONFIG, storage)
                .electionJitter(_ -> DETERMINISTIC_JITTER)
                .build();
        }

        @Test
        void learnerDoesNotStartElection() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            for (int i = 0; i < 50; i++) {
                learner.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.FOLLOWER, learner.role());
            assertEquals(0, learner.term());
        }

        @Test
        void learnerReceivesAppendEntries() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var request = new RaftMessage.AppendEntries(
                1, "n1", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            var ready = learner.step(new RaftInput.MessageReceived("n1", request));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(1, response.matchIndex());
        }

        @Test
        void learnerAdvancesCommitIndex() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var request = new RaftMessage.AppendEntries(
                1, "n1", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                1
            );
            learner.step(new RaftInput.MessageReceived("n1", request));

            assertEquals(1, learner.commitIndex());
        }

        @Test
        void learnerDoesNotCountInQuorum() {
            var configuration = new RaftMembership(Set.of("n1", "n2", "n3"), Set.of("n4"));
            var leader = createRaft("n1", configuration, CONFIG);
            becomeLeader(leader, List.of("n2", "n3"));

            leader.step(new RaftInput.EntryProposed(bytes("hello")));
            assertEquals(0, leader.commitIndex());

            var learnerResponse = new RaftMessage.AppendEntriesResponse(1, true, 2);
            leader.step(new RaftInput.MessageReceived("n4", learnerResponse));
            assertEquals(0, leader.commitIndex());

            var voterResponse = new RaftMessage.AppendEntriesResponse(1, true, 2);
            leader.step(new RaftInput.MessageReceived("n2", voterResponse));
            assertEquals(2, leader.commitIndex());
        }

        @Test
        void learnerCanForwardReadIndex() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var heartbeat = new RaftMessage.AppendEntries(1, "n1", 0, 0, List.of(), 0);
            learner.step(new RaftInput.MessageReceived("n1", heartbeat));
            assertEquals("n1", learner.leaderId().orElseThrow());

            var ready = learner.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var readIndexMsg = ready.messages().stream()
                .filter(m -> m.to().equals("n1"))
                .filter(m -> m.message() instanceof RaftMessage.ReadIndexRequested)
                .map(m -> (RaftMessage.ReadIndexRequested) m.message())
                .findFirst();

            assertTrue(readIndexMsg.isPresent());
            assertEquals(bytes("req-1"), readIndexMsg.get().context());
        }

        @Test
        void learnerReceivesReadIndexResponse() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var heartbeat = new RaftMessage.AppendEntries(1, "n1", 0, 0, List.of(), 0);
            learner.step(new RaftInput.MessageReceived("n1", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 5, bytes("req-1"));
            var ready = learner.step(new RaftInput.MessageReceived("n1", response));

            assertEquals(1, ready.application().readStates().size());
            assertEquals(5, ready.application().readStates().get(0).index());
            assertEquals(bytes("req-1"), ready.application().readStates().get(0).context());
        }

        @Test
        void learnerDoesNotReceiveVoteRequests() {
            var configuration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
            var voter = createRaft("n1", configuration, CONFIG);

            var ready = tickUntilTimeout(voter);
            assertEquals(RaftRole.PRE_CANDIDATE, voter.role());

            var preVoteTargets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.PreVote)
                .map(RaftEffects.Outbound::to)
                .toList();

            assertFalse(preVoteTargets.contains("n3"));
            assertTrue(preVoteTargets.contains("n2"));
        }

        @Test
        void leaderReplicatesToLearner() {
            var configuration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
            var leader = createRaft("n1", configuration, CONFIG);
            becomeLeader(leader, List.of("n2"));

            var ready = leader.step(new RaftInput.EntryProposed(bytes("hello")));

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(RaftEffects.Outbound::to)
                .toList();

            assertTrue(targets.contains("n2"));
            assertTrue(targets.contains("n3"));
        }
    }

    @Nested
    class MembershipChanges {
        @Test
        void leaderAddsLearner() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n4"));
        }

        @Test
        void leaderPromotesLearner() {
            var configuration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
            var raft = createRaft("n1", configuration, CONFIG);
            becomeLeader(raft, List.of("n2"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.PromoteToVoter("n3")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isVoter("n3"));
            assertFalse(configEntry.get().membership().isLearner("n3"));
        }

        @Test
        void leaderDemotesVoter() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n3")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n3"));
            assertFalse(configEntry.get().membership().isVoter("n3"));
        }

        @Test
        void leaderRemovesNode() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n3")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertFalse(configEntry.get().membership().isMember("n3"));
        }

        @Test
        void nonLeaderIgnoresConfigChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));

            assertTrue(ready.persistence().entries().isEmpty());
            assertTrue(ready.messages().isEmpty());
        }

        @Test
        void rejectSecondPendingConfigChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n5")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void singleNodeLeaderCommitsConfigImmediately() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n2")));

            assertEquals(1, ready.application().membershipTransitions().size());
            var change = ready.application().membershipTransitions().getFirst();
            assertFalse(change.previous().isMember("n2"));
            assertTrue(change.current().isLearner("n2"));
        }

        @Test
        void configurationChangeUpdatesQuorum() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n3")));

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderStepsDownAfterSelfRemoval() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n1")));

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void removedNodeDoesNotCampaignAfterSelfRemoval() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n1")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            for (int i = 0; i < CONFIG.electionTimeoutMax() * 2; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void leaderStepsDownAfterSelfDemotion() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n1")));

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void configurationChangeAppliesOnCommit() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));
            assertFalse(raft.membership().isLearner("n4"));

            var ready = raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertTrue(raft.membership().isLearner("n4"));
            assertEquals(1, ready.application().membershipTransitions().size());
        }

        @Test
        void cannotDemoteLastVoter() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n1")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotRemoveLastVoter() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n1")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotAddExistingMember() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n2")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotPromoteNonLearner() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.PromoteToVoter("n4")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void allowsSecondConfigChangeAfterFirstCommits() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n5")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n5"));
        }

        @Test
        void newPeerAddedToReplication() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = tickHeartbeat(raft);

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(RaftEffects.Outbound::to)
                .toList();
            assertTrue(targets.contains("n4"));
        }

        @Test
        void removedPeerExcludedFromReplication() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n3")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = tickHeartbeat(raft);

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(RaftEffects.Outbound::to)
                .toList();
            assertFalse(targets.contains("n3"));
        }
    }
}
