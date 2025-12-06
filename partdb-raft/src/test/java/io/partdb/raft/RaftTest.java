package io.partdb.raft;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();
    private static final int DETERMINISTIC_JITTER = 0;

    private Raft createRaft(String id, String... allVoters) {
        var membership = Membership.ofVoters(allVoters);
        var storage = new InMemoryStorage(membership);
        return new Raft(id, membership, CONFIG, storage, _ -> DETERMINISTIC_JITTER);
    }

    private Raft createRaft(String id, Membership membership, RaftConfig config) {
        var storage = new InMemoryStorage(membership);
        return new Raft(id, membership, config, storage, _ -> DETERMINISTIC_JITTER);
    }

    private Raft createRaftWithLog(String id, List<LogEntry> entries, HardState hardState, String... allVoters) {
        var membership = Membership.ofVoters(allVoters);
        var storage = new InMemoryStorage(membership);
        storage.append(null, entries);
        var raft = new Raft(id, membership, CONFIG, storage, _ -> DETERMINISTIC_JITTER);
        raft.restore(hardState, 0);
        return raft;
    }

    private Raft createRaftWithSnapshot(String id, HardState hardState, long snapIndex, String... allVoters) {
        var membership = Membership.ofVoters(allVoters);
        var storage = new InMemoryStorage(membership);
        var raft = new Raft(id, membership, CONFIG, storage, _ -> DETERMINISTIC_JITTER);
        raft.restore(hardState, snapIndex);
        return raft;
    }

    private Raft createRaftWithCompactedSnapshot(String id, long snapIndex, long snapTerm, String... allVoters) {
        var membership = Membership.ofVoters(allVoters);
        var storage = new InMemoryStorage(membership);
        for (int i = 1; i <= snapIndex; i++) {
            storage.append(null, List.of(new LogEntry.Data(i, snapTerm, new byte[0])));
        }
        storage.saveSnapshot(new Snapshot(snapIndex, snapTerm, membership, new byte[0]));
        var raft = new Raft(id, membership, CONFIG, storage, _ -> DETERMINISTIC_JITTER);
        raft.restore(HardState.INITIAL, snapIndex);
        return raft;
    }

    private Ready tickUntilTimeout(Raft raft) {
        Ready ready = null;
        for (int i = 0; i < CONFIG.electionTimeoutMin(); i++) {
            ready = raft.step(new RaftEvent.Tick());
        }
        return ready;
    }

    private Ready grantPreVotes(Raft raft, List<String> peers) {
        long term = raft.term();
        Ready ready = null;
        for (String peer : peers) {
            ready = raft.step(new RaftEvent.Receive(peer, new RaftMessage.PreVoteResponse(term, true)));
            if (raft.role() == Role.CANDIDATE) break;
        }
        return ready;
    }

    private Ready grantVotes(Raft raft, List<String> peers) {
        long term = raft.term();
        Ready ready = null;
        for (String peer : peers) {
            ready = raft.step(new RaftEvent.Receive(peer, new RaftMessage.RequestVoteResponse(term, true)));
            if (raft.isLeader()) break;
        }
        return ready;
    }

    private Ready tickUntilCandidate(Raft raft, List<String> peers) {
        tickUntilTimeout(raft);
        return grantPreVotes(raft, peers);
    }

    private Ready becomeLeader(Raft raft, List<String> peers) {
        tickUntilCandidate(raft, peers);
        return grantVotes(raft, peers);
    }

    private <T extends RaftMessage.Response> Optional<T> findResponse(Ready ready, Class<T> type) {
        return ready.messages().stream()
            .map(Ready.Outbound::message)
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    private <T extends RaftMessage.Request> Optional<T> findRequest(Ready ready, Class<T> type) {
        return ready.messages().stream()
            .map(Ready.Outbound::message)
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    private <T extends LogEntry> Optional<T> findEntry(Ready ready, Class<T> type) {
        return ready.persist().entries().stream()
            .filter(type::isInstance)
            .map(type::cast)
            .findFirst();
    }

    private List<RaftMessage.AppendEntries> getAppendEntries(Ready ready) {
        return ready.messages().stream()
            .map(Ready.Outbound::message)
            .filter(RaftMessage.AppendEntries.class::isInstance)
            .map(RaftMessage.AppendEntries.class::cast)
            .toList();
    }

    private List<RaftMessage.PreVote> getPreVotes(Ready ready) {
        return ready.messages().stream()
            .map(Ready.Outbound::message)
            .filter(RaftMessage.PreVote.class::isInstance)
            .map(RaftMessage.PreVote.class::cast)
            .toList();
    }

    private List<RaftMessage.RequestVote> getRequestVotes(Ready ready) {
        return ready.messages().stream()
            .map(Ready.Outbound::message)
            .filter(RaftMessage.RequestVote.class::isInstance)
            .map(RaftMessage.RequestVote.class::cast)
            .toList();
    }

    private Ready tickHeartbeat(Raft raft) {
        Ready ready = null;
        for (int i = 0; i < CONFIG.heartbeatInterval(); i++) {
            ready = raft.step(new RaftEvent.Tick());
        }
        return ready;
    }

    @Nested
    class InitialState {
        @Test
        void startsAsFollower() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(Role.FOLLOWER, raft.role());
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
                raft.step(new RaftEvent.Tick());
            }
            assertEquals(Role.FOLLOWER, raft.role());

            raft.step(new RaftEvent.Tick());
            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void candidateRestartsElectionAfterTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(Role.CANDIDATE, raft.role());
            assertEquals(1, raft.term());

            tickUntilTimeout(raft);
            assertEquals(Role.CANDIDATE, raft.role());
            assertEquals(2, raft.term());
        }

        @Test
        void leaderDoesNotTimeoutElection() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());
            long term = raft.term();

            for (int i = 0; i < 50; i++) {
                raft.step(new RaftEvent.Tick());
            }

            assertTrue(raft.isLeader());
            assertEquals(term, raft.term());
        }

        @Test
        void electionTimeoutIsRandomized() {
            var counter = new AtomicInteger(0);
            var membership = Membership.ofVoters("n1", "n2");
            var storage = new InMemoryStorage(membership);
            var raft = new Raft("n1", membership, RaftConfig.defaults(), storage, _ -> counter.incrementAndGet());

            for (int i = 0; i < 50; i++) {
                raft.step(new RaftEvent.Tick());
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
            var ready = raft.step(new RaftEvent.Receive("n2", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response.voteGranted());
        }

        @Test
        void rejectsVoteIfAlreadyVotedForAnother() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request1 = new RaftMessage.RequestVote(1, "n2", 0, 0);
            raft.step(new RaftEvent.Receive("n2", request1));

            var request2 = new RaftMessage.RequestVote(1, "n3", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n3", request2));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void rejectsVoteIfCandidateLogBehind() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                0
            );
            raft.step(new RaftEvent.Receive("n2", appendEntries));

            var request = new RaftMessage.RequestVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void rejectsVoteFromLowerTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", higherTermMsg));

            var request = new RaftMessage.RequestVote(3, "n3", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void votingPersistsHardState() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n2", request));

            assertNotNull(ready.persist().hardState());
            assertEquals(1, ready.persist().hardState().term());
            assertEquals("n2", ready.persist().hardState().votedFor());
        }
    }

    @Nested
    class LeaderElection {
        @Test
        void candidateBecomesLeaderWithMajority() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(Role.CANDIDATE, raft.role());

            raft.step(new RaftEvent.Receive("n2",
                new RaftMessage.RequestVoteResponse(1, true)));

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

            assertFalse(ready.persist().entries().isEmpty());
            var noOp = ready.persist().entries().stream()
                .filter(e -> e instanceof LogEntry.NoOp)
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
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", higherTermMsg));

            assertEquals(Role.FOLLOWER, raft.role());
            assertEquals(5, raft.term());
        }

        @Test
        void rejectMessagesFromLowerTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", higherTermMsg));

            var lowerTermAppend = new RaftMessage.AppendEntries(3, "n3", 0, 0, List.of(), 0);
            var ready = raft.step(new RaftEvent.Receive("n3", lowerTermAppend));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
            assertEquals(5, response.term());
        }

        @Test
        void updateTermOnHigherTermResponse() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertEquals(1, raft.term());

            var higherTermResponse = new RaftMessage.AppendEntriesResponse(5, false, 0);
            raft.step(new RaftEvent.Receive("n2", higherTermResponse));

            assertEquals(5, raft.term());
            assertEquals(Role.FOLLOWER, raft.role());
        }
    }

    @Nested
    class AppendEntries {
        @Test
        void followerAcceptsMatchingAppendEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                0
            );
            var ready = raft.step(new RaftEvent.Receive("n2", request));

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
            var ready = raft.step(new RaftEvent.Receive("n2", request));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
        }

        @Test
        void followerAdvancesCommitIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                1
            );
            raft.step(new RaftEvent.Receive("n2", request));

            assertEquals(1, raft.commitIndex());
        }

        @Test
        void followerResetsElectionTimerOnAppendEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftEvent.Tick());
            }

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", heartbeat));

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftEvent.Tick());
            }

            assertEquals(Role.FOLLOWER, raft.role());
        }

        @Test
        void followerPersistsNewEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                0
            );
            var ready = raft.step(new RaftEvent.Receive("n2", request));

            assertEquals(1, ready.persist().entries().size());
        }
    }

    @Nested
    class LogReplication {
        @Test
        void leaderBroadcastsAppendEntriesOnPropose() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            var ready = raft.step(new RaftEvent.Propose("hello".getBytes()));

            long appendCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .count();
            assertEquals(2, appendCount);
        }

        @Test
        void leaderAdvancesCommitOnMajorityMatch() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            raft.step(new RaftEvent.Propose("hello".getBytes()));

            var response = new RaftMessage.AppendEntriesResponse(1, true, 2);
            raft.step(new RaftEvent.Receive("n2", response));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderDecrementsNextIndexOnReject() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftEvent.Receive("n2", reject));

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

            raft.step(new RaftEvent.Propose("hello".getBytes()));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void nonLeaderIgnoresPropose() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftEvent.Propose("hello".getBytes()));

            assertTrue(ready.persist().entries().isEmpty());
            assertTrue(ready.messages().isEmpty());
        }
    }

    @Nested
    class Heartbeats {
        @Test
        void leaderSendsHeartbeatsAtInterval() {
            var config = new RaftConfig(10, 20, 3, 100);
            var raft = createRaft("n1", Membership.ofVoters("n1", "n2", "n3"), config);
            becomeLeader(raft, List.of("n2"));

            for (int i = 0; i < 2; i++) {
                raft.step(new RaftEvent.Tick());
            }

            var ready = raft.step(new RaftEvent.Tick());

            long heartbeatCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .count();
            assertEquals(2, heartbeatCount);
        }

        @Test
        void heartbeatContainsCommitIndex() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);

            raft.step(new RaftEvent.Propose("data".getBytes()));

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
                1, "n2", 10, 1, Membership.ofVoters("n1", "n2", "n3"), "snapshot-data".getBytes()
            );
            var ready = raft.step(new RaftEvent.Receive("n2", snapshot));

            assertNotNull(ready.persist().incomingSnapshot());
            assertEquals(10, ready.persist().incomingSnapshot().index());
        }

        @Test
        void followerRejectsSnapshotBelowCommitIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var entries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                1
            );
            raft.step(new RaftEvent.Receive("n2", entries));
            assertEquals(1, raft.commitIndex());

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 1, 1, Membership.ofVoters("n1", "n2", "n3"), "snapshot-data".getBytes()
            );
            var ready = raft.step(new RaftEvent.Receive("n2", snapshot));

            assertNull(ready.persist().incomingSnapshot());
        }

        @Test
        void leaderSendsSnapshotWhenFollowerBehindCompactedLog() {
            var raft = createRaftWithCompactedSnapshot("n1", 1, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftEvent.Receive("n2", reject));

            assertNotNull(ready.snapshotToSend());
            assertEquals("n2", ready.snapshotToSend().peer());
        }

        @Test
        void leaderUpdatesIndicesOnInstallSnapshotResponse() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var response = new RaftMessage.InstallSnapshotResponse(raft.term());
            raft.step(new RaftEvent.Receive("n2", response));

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
            var snapshotReady = raft.step(new RaftEvent.Receive("n2", reject));
            assertNotNull(snapshotReady.snapshotToSend());
            assertEquals("n2", snapshotReady.snapshotToSend().peer());

            var oldTermResponse = new RaftMessage.InstallSnapshotResponse(0);
            raft.step(new RaftEvent.Receive("n2", oldTermResponse));

            var ready = tickHeartbeat(raft);

            var snapshotToN2 = ready.snapshotToSend();
            assertNotNull(snapshotToN2);
            assertEquals("n2", snapshotToN2.peer());
        }

        @Test
        void leaderIgnoresInstallSnapshotResponseWhenNotLeader() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var higherTermMsg = new RaftMessage.AppendEntries(10, "n3", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n3", higherTermMsg));
            assertEquals(Role.FOLLOWER, raft.role());

            var response = new RaftMessage.InstallSnapshotResponse(1);
            var ready = raft.step(new RaftEvent.Receive("n2", response));

            assertTrue(ready.messages().isEmpty());
        }

        @Test
        void installSnapshotFromLowerTermIsRejected() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", higherTermMsg));
            assertEquals(5, raft.term());

            var snapshot = new RaftMessage.InstallSnapshot(
                3, "n3", 10, 3, Membership.ofVoters("n1", "n2", "n3"), "snapshot-data".getBytes()
            );
            var ready = raft.step(new RaftEvent.Receive("n3", snapshot));

            assertNull(ready.persist().incomingSnapshot());

            var response = findResponse(ready, RaftMessage.InstallSnapshotResponse.class).orElseThrow();
            assertEquals(5, response.term());
        }

        @Test
        void installSnapshotResetsLeaderContactTicks() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 5; i++) {
                raft.step(new RaftEvent.Tick());
            }

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 10, 1, Membership.ofVoters("n1", "n2", "n3"), "snapshot-data".getBytes()
            );
            raft.step(new RaftEvent.Receive("n2", snapshot));

            var preVote = new RaftMessage.PreVote(2, "n3", 10, 1);
            var ready = raft.step(new RaftEvent.Receive("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }
    }

    @Nested
    class CommitSafety {
        @Test
        void leaderDoesNotCommitEntriesFromPreviousTerm() {
            var oldEntry = new LogEntry.Data(1, 1, "old".getBytes());
            var raft = createRaftWithLog("n1", List.of(oldEntry), new HardState(1, null, 0), "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(Role.CANDIDATE, raft.role());
            assertEquals(2, raft.term());

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.RequestVoteResponse(2, true)));
            assertTrue(raft.isLeader());

            var response = new RaftMessage.AppendEntriesResponse(2, true, 1);
            raft.step(new RaftEvent.Receive("n2", response));

            assertEquals(0, raft.commitIndex());
        }

        @Test
        void leaderCommitsCurrentTermEntryImplicitlyCommittingPrevious() {
            var oldEntry = new LogEntry.Data(1, 1, "old".getBytes());
            var raft = createRaftWithLog("n1", List.of(oldEntry), new HardState(1, null, 0), "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            raft.step(new RaftEvent.Receive("n2", new RaftMessage.RequestVoteResponse(2, true)));
            assertTrue(raft.isLeader());

            var response = new RaftMessage.AppendEntriesResponse(2, true, 2);
            raft.step(new RaftEvent.Receive("n2", response));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderRequiresMajorityForCurrentTermEntry() {
            var oldEntry = new LogEntry.Data(1, 1, "old".getBytes());
            var raft = createRaftWithLog("n1", List.of(oldEntry), new HardState(1, null, 0), "n1", "n2", "n3", "n4", "n5");

            tickUntilCandidate(raft, List.of("n2", "n3", "n4", "n5"));
            raft.step(new RaftEvent.Receive("n2", new RaftMessage.RequestVoteResponse(2, true)));
            raft.step(new RaftEvent.Receive("n3", new RaftMessage.RequestVoteResponse(2, true)));
            assertTrue(raft.isLeader());

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(2, true, 2)));
            assertEquals(0, raft.commitIndex());

            raft.step(new RaftEvent.Receive("n3", new RaftMessage.AppendEntriesResponse(2, true, 2)));
            assertEquals(2, raft.commitIndex());
        }
    }

    @Nested
    class PersistenceRecovery {
        @Test
        void restoresTermFromHardState() {
            var raft = createRaftWithSnapshot("n1", new HardState(5, null, 0), 0, "n1", "n2", "n3");

            assertEquals(5, raft.term());
        }

        @Test
        void restoresVotedForFromHardState() {
            var raft = createRaftWithSnapshot("n1", new HardState(3, "n2", 0), 0, "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(3, "n3", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void restoresLogEntries() {
            List<LogEntry> entries = List.of(
                new LogEntry.Data(1, 1, "first".getBytes()),
                new LogEntry.Data(2, 1, "second".getBytes())
            );
            var raft = createRaftWithLog("n1", entries, new HardState(1, null, 0), "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            var ready = raft.step(new RaftEvent.Receive("n2",
                new RaftMessage.RequestVoteResponse(2, true)));

            var append = ready.messages().stream()
                .map(Ready.Outbound::message)
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
            var raft = createRaftWithSnapshot("n1", new HardState(2, null, 0), 5, "n1", "n2", "n3");

            assertEquals(5, raft.commitIndex());
        }

        @Test
        void restoredNodeCanBecomeLeader() {
            List<LogEntry> entries = List.of(new LogEntry.Data(1, 1, "data".getBytes()));
            var raft = createRaftWithLog("n1", entries, new HardState(1, null, 0), "n1");

            tickUntilTimeout(raft);

            assertTrue(raft.isLeader());
            assertEquals(2, raft.term());

            raft.step(new RaftEvent.Propose("new-data".getBytes()));
            assertEquals(3, raft.commitIndex());
        }

        @Test
        void restoredNodeRejectsOldTermMessages() {
            var raft = createRaftWithSnapshot("n1", new HardState(10, null, 0), 0, "n1", "n2", "n3");

            var oldAppend = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            var ready = raft.step(new RaftEvent.Receive("n2", oldAppend));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
            assertEquals(10, response.term());
        }
    }

    @Nested
    class LogConflictResolution {
        @Test
        void followerTruncatesConflictingEntries() {
            List<LogEntry> entries = List.of(
                new LogEntry.Data(1, 1, "a".getBytes()),
                new LogEntry.Data(2, 1, "b".getBytes()),
                new LogEntry.Data(3, 1, "c".getBytes())
            );
            var raft = createRaftWithLog("n1", entries, new HardState(1, null, 0), "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                2, "n2", 1, 1,
                List.of(
                    new LogEntry.Data(2, 2, "new-b".getBytes()),
                    new LogEntry.Data(3, 2, "new-c".getBytes())
                ),
                0
            );
            var ready = raft.step(new RaftEvent.Receive("n2", appendEntries));

            assertEquals(2, ready.persist().entries().size());
            assertEquals(2, ready.persist().entries().get(0).term());
            assertEquals(2, ready.persist().entries().get(1).term());

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(3, response.matchIndex());
        }

        @Test
        void followerAcceptsEntriesAfterConflictResolution() {
            List<LogEntry> entries = List.of(
                new LogEntry.Data(1, 1, "a".getBytes()),
                new LogEntry.Data(2, 1, "b".getBytes())
            );
            var raft = createRaftWithLog("n1", entries, new HardState(1, null, 0), "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                2, "n2", 1, 1,
                List.of(
                    new LogEntry.Data(2, 2, "new-b".getBytes()),
                    new LogEntry.Data(3, 2, "new-c".getBytes()),
                    new LogEntry.Data(4, 2, "new-d".getBytes())
                ),
                0
            );
            var ready = raft.step(new RaftEvent.Receive("n2", appendEntries));

            assertEquals(3, ready.persist().entries().size());

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(4, response.matchIndex());
        }

        @Test
        void followerRejectsIfPrevLogDoesNotMatch() {
            List<LogEntry> entries = List.of(
                new LogEntry.Data(1, 1, "a".getBytes()),
                new LogEntry.Data(2, 1, "b".getBytes())
            );
            var raft = createRaftWithLog("n1", entries, new HardState(1, null, 0), "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                2, "n2", 2, 2,
                List.of(new LogEntry.Data(3, 2, "c".getBytes())),
                0
            );
            var ready = raft.step(new RaftEvent.Receive("n2", appendEntries));

            assertTrue(ready.persist().entries().isEmpty());

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
        }

        @Test
        void leaderRetriesWithLowerNextIndexOnReject() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftEvent.Receive("n2", reject));

            var retry = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .map(m -> (RaftMessage.AppendEntries) m.message())
                .findFirst()
                .orElse(null);

            assertNotNull(retry);
            assertEquals(0, retry.prevLogIndex());

            var accept = new RaftMessage.AppendEntriesResponse(1, true, 1);
            raft.step(new RaftEvent.Receive("n2", accept));

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
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                0
            );

            var ready1 = raft.step(new RaftEvent.Receive("n2", appendEntries));
            assertEquals(1, ready1.persist().entries().size());
            var response1 = findResponse(ready1, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response1.success());
            assertEquals(1, response1.matchIndex());

            var ready2 = raft.step(new RaftEvent.Receive("n2", appendEntries));
            assertTrue(ready2.persist().entries().isEmpty());
            var response2 = findResponse(ready2, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response2.success());
            assertEquals(1, response2.matchIndex());
        }

        @Test
        void duplicateRequestVoteGrantsToSameCandidate() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var requestVote = new RaftMessage.RequestVote(1, "n2", 0, 0);

            var ready1 = raft.step(new RaftEvent.Receive("n2", requestVote));
            var response1 = findResponse(ready1, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response1.voteGranted());

            var ready2 = raft.step(new RaftEvent.Receive("n2", requestVote));
            var response2 = findResponse(ready2, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response2.voteGranted());
        }

        @Test
        void duplicateAppendEntriesResponseOnlyCountsOnce() {
            var raft = createRaft("n1", "n1", "n2", "n3", "n4", "n5");
            becomeLeader(raft, List.of("n2", "n3"));

            var response = new RaftMessage.AppendEntriesResponse(1, true, 1);

            raft.step(new RaftEvent.Receive("n2", response));
            assertEquals(0, raft.commitIndex());

            raft.step(new RaftEvent.Receive("n2", response));
            assertEquals(0, raft.commitIndex());

            raft.step(new RaftEvent.Receive("n3", response));
            assertEquals(1, raft.commitIndex());
        }
    }

    @Nested
    class VoteSplitting {
        @Test
        void electionRestartsWhenNoMajority() {
            var raft = createRaft("n1", "n1", "n2", "n3", "n4", "n5");

            tickUntilCandidate(raft, List.of("n2", "n3", "n4", "n5"));
            assertEquals(Role.CANDIDATE, raft.role());
            assertEquals(1, raft.term());

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertEquals(Role.CANDIDATE, raft.role());

            tickUntilTimeout(raft);
            assertEquals(Role.CANDIDATE, raft.role());
            assertEquals(2, raft.term());
        }

        @Test
        void candidateVotesForItself() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(Role.CANDIDATE, raft.role());

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertTrue(raft.isLeader());
        }

        @Test
        void splitVoteLeadsToNewElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(1, raft.term());

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.RequestVoteResponse(1, false)));
            raft.step(new RaftEvent.Receive("n3", new RaftMessage.RequestVoteResponse(1, false)));
            assertEquals(Role.CANDIDATE, raft.role());

            tickUntilTimeout(raft);
            assertEquals(2, raft.term());
            assertEquals(Role.CANDIDATE, raft.role());
        }
    }

    @Nested
    class PreVote {
        @Test
        void followerStartsPreVoteOnTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftEvent.Tick());
            }
            assertEquals(Role.FOLLOWER, raft.role());

            raft.step(new RaftEvent.Tick());
            assertEquals(Role.PRE_CANDIDATE, raft.role());
        }

        @Test
        void preVoteDoesNotIncrementTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);

            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void preVoteDoesNotPersistHardState() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftEvent.Tick());
            for (int i = 1; i < 10; i++) {
                ready = raft.step(new RaftEvent.Tick());
            }

            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertNull(ready.persist().hardState());
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
            assertEquals(Role.PRE_CANDIDATE, raft.role());

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", heartbeat));

            assertEquals(Role.FOLLOWER, raft.role());
            assertEquals("n2", raft.leaderId().orElseThrow());
        }

        @Test
        void preCandidateBecomesFollowerOnHigherTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", higherTermMsg));

            assertEquals(Role.FOLLOWER, raft.role());
            assertEquals(5, raft.term());
        }

        @Test
        void preCandidateBecomesCandidateWithMajorityPreVotes() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.PreVoteResponse(0, true)));

            assertEquals(Role.CANDIDATE, raft.role());
            assertEquals(1, raft.term());
        }

        @Test
        void preVoteGrantedWhenLogUpToDateAndNoLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);

            var preVote = new RaftMessage.PreVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n2", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertTrue(response.voteGranted());
        }

        @Test
        void preVoteDeniedWhenLeaderActive() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", heartbeat));

            var preVote = new RaftMessage.PreVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void preVoteDeniedWhenLogBehind() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                0
            );
            raft.step(new RaftEvent.Receive("n2", appendEntries));

            tickUntilTimeout(raft);

            var preVote = new RaftMessage.PreVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftEvent.Receive("n3", preVote));

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
            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            tickUntilTimeout(raft);
            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void preVoteDoesNotUpdateTermOnHigherTermPreVote() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            assertEquals(0, raft.term());

            var preVote = new RaftMessage.PreVote(10, "n2", 0, 0);
            raft.step(new RaftEvent.Receive("n2", preVote));

            assertEquals(0, raft.term());
            assertEquals(Role.FOLLOWER, raft.role());
        }

        @Test
        void preVoteResponseDoesNotUpdateTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(Role.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            var response = new RaftMessage.PreVoteResponse(10, false);
            raft.step(new RaftEvent.Receive("n2", response));

            assertEquals(0, raft.term());
        }
    }

    @Nested
    class ReadIndex {
        @Test
        void leaderConfirmsReadIndexImmediatelyForSingleNode() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            assertEquals(1, ready.apply().readStates().size());
            var readState = ready.apply().readStates().get(0);
            assertEquals(raft.commitIndex(), readState.index());
            assertArrayEquals("req-1".getBytes(), readState.context());
        }

        @Test
        void leaderBroadcastsHeartbeatForReadIndexConfirmation() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            assertTrue(ready.apply().readStates().isEmpty());
            var appendEntries = getAppendEntries(ready);
            assertEquals(2, appendEntries.size());
        }

        @Test
        void leaderConfirmsReadIndexOnQuorumAck() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            var ackReady = raft.step(new RaftEvent.Receive("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            assertEquals(1, ackReady.apply().readStates().size());
            assertArrayEquals("req-1".getBytes(), ackReady.apply().readStates().get(0).context());
        }

        @Test
        void leaderRespondsToForwardedReadIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            long commitIndexAtRequest = raft.commitIndex();
            var readIndexMsg = new RaftMessage.ReadIndex(raft.term(), "req-1".getBytes());
            raft.step(new RaftEvent.Receive("n2", readIndexMsg));

            var ackReady = raft.step(new RaftEvent.Receive("n3",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            var response = findResponse(ackReady, RaftMessage.ReadIndexResponse.class);
            assertTrue(response.isPresent());
            assertEquals(commitIndexAtRequest, response.get().readIndex());
            assertArrayEquals("req-1".getBytes(), response.get().context());
        }

        @Test
        void leaderClearsPendingReadsOnBecomeFollower() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            var higherTermMsg = new RaftMessage.AppendEntries(10, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", higherTermMsg));
            assertEquals(Role.FOLLOWER, raft.role());

            var ackReady = raft.step(new RaftEvent.Receive("n3",
                new RaftMessage.AppendEntriesResponse(1, true, 1)));

            assertTrue(ackReady.apply().readStates().isEmpty());
        }

        @Test
        void followerForwardsReadIndexToLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", heartbeat));
            assertEquals("n2", raft.leaderId().orElseThrow());

            var ready = raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            var readIndexMsg = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .filter(m -> m.message() instanceof RaftMessage.ReadIndex)
                .map(m -> (RaftMessage.ReadIndex) m.message())
                .findFirst();

            assertTrue(readIndexMsg.isPresent());
            assertArrayEquals("req-1".getBytes(), readIndexMsg.get().context());
        }

        @Test
        void followerEmitsReadStateOnResponse() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 5, "req-1".getBytes());
            var ready = raft.step(new RaftEvent.Receive("n2", response));

            assertEquals(1, ready.apply().readStates().size());
            assertEquals(5, ready.apply().readStates().get(0).index());
            assertArrayEquals("req-1".getBytes(), ready.apply().readStates().get(0).context());
        }

        @Test
        void followerDropsReadIndexWhenNoLeaderKnown() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertTrue(raft.leaderId().isEmpty());

            var ready = raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            assertTrue(ready.messages().isEmpty());
            assertTrue(ready.apply().readStates().isEmpty());
        }

        @Test
        void readIndexResponseWithZeroIndexIndicatesFailure() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftEvent.Receive("n2", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 0, "req-1".getBytes());
            var ready = raft.step(new RaftEvent.Receive("n2", response));

            assertTrue(ready.apply().readStates().isEmpty());
        }

        @Test
        void nonLeaderRespondsWithZeroReadIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var readIndexMsg = new RaftMessage.ReadIndex(1, "req-1".getBytes());
            var ready = raft.step(new RaftEvent.Receive("n2", readIndexMsg));

            var response = findResponse(ready, RaftMessage.ReadIndexResponse.class).orElseThrow();
            assertEquals(0, response.readIndex());
        }

        @Test
        void multipleReadIndexRequestsTrackedIndependently() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ReadIndex("req-1".getBytes()));
            raft.step(new RaftEvent.ReadIndex("req-2".getBytes()));

            var ackReady = raft.step(new RaftEvent.Receive("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            assertEquals(2, ackReady.apply().readStates().size());
        }

        @Test
        void readIndexDoesNotTriggerTermChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());

            var readIndexMsg = new RaftMessage.ReadIndex(10, "req-1".getBytes());
            raft.step(new RaftEvent.Receive("n2", readIndexMsg));

            assertEquals(0, raft.term());
        }

        @Test
        void readIndexResponseDoesNotTriggerTermChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());

            var response = new RaftMessage.ReadIndexResponse(10, 5, "req-1".getBytes());
            raft.step(new RaftEvent.Receive("n2", response));

            assertEquals(0, raft.term());
        }
    }

    @Nested
    class Learners {
        private Raft createLearner(String id, Set<String> voters, Set<String> learners) {
            var membership = new Membership(voters, learners);
            var storage = new InMemoryStorage(membership);
            return new Raft(id, membership, CONFIG, storage, _ -> DETERMINISTIC_JITTER);
        }

        @Test
        void learnerDoesNotStartElection() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            for (int i = 0; i < 50; i++) {
                learner.step(new RaftEvent.Tick());
            }

            assertEquals(Role.FOLLOWER, learner.role());
            assertEquals(0, learner.term());
        }

        @Test
        void learnerReceivesAppendEntries() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var request = new RaftMessage.AppendEntries(
                1, "n1", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                0
            );
            var ready = learner.step(new RaftEvent.Receive("n1", request));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(1, response.matchIndex());
        }

        @Test
        void learnerAdvancesCommitIndex() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var request = new RaftMessage.AppendEntries(
                1, "n1", 0, 0,
                List.of(new LogEntry.Data(1, 1, "data".getBytes())),
                1
            );
            learner.step(new RaftEvent.Receive("n1", request));

            assertEquals(1, learner.commitIndex());
        }

        @Test
        void learnerDoesNotCountInQuorum() {
            var membership = new Membership(Set.of("n1", "n2", "n3"), Set.of("n4"));
            var leader = createRaft("n1", membership, CONFIG);
            becomeLeader(leader, List.of("n2", "n3"));

            leader.step(new RaftEvent.Propose("hello".getBytes()));
            assertEquals(0, leader.commitIndex());

            var learnerResponse = new RaftMessage.AppendEntriesResponse(1, true, 2);
            leader.step(new RaftEvent.Receive("n4", learnerResponse));
            assertEquals(0, leader.commitIndex());

            var voterResponse = new RaftMessage.AppendEntriesResponse(1, true, 2);
            leader.step(new RaftEvent.Receive("n2", voterResponse));
            assertEquals(2, leader.commitIndex());
        }

        @Test
        void learnerCanForwardReadIndex() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var heartbeat = new RaftMessage.AppendEntries(1, "n1", 0, 0, List.of(), 0);
            learner.step(new RaftEvent.Receive("n1", heartbeat));
            assertEquals("n1", learner.leaderId().orElseThrow());

            var ready = learner.step(new RaftEvent.ReadIndex("req-1".getBytes()));

            var readIndexMsg = ready.messages().stream()
                .filter(m -> m.to().equals("n1"))
                .filter(m -> m.message() instanceof RaftMessage.ReadIndex)
                .map(m -> (RaftMessage.ReadIndex) m.message())
                .findFirst();

            assertTrue(readIndexMsg.isPresent());
            assertArrayEquals("req-1".getBytes(), readIndexMsg.get().context());
        }

        @Test
        void learnerReceivesReadIndexResponse() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var heartbeat = new RaftMessage.AppendEntries(1, "n1", 0, 0, List.of(), 0);
            learner.step(new RaftEvent.Receive("n1", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 5, "req-1".getBytes());
            var ready = learner.step(new RaftEvent.Receive("n1", response));

            assertEquals(1, ready.apply().readStates().size());
            assertEquals(5, ready.apply().readStates().get(0).index());
            assertArrayEquals("req-1".getBytes(), ready.apply().readStates().get(0).context());
        }

        @Test
        void learnerDoesNotReceiveVoteRequests() {
            var membership = new Membership(Set.of("n1", "n2"), Set.of("n3"));
            var voter = createRaft("n1", membership, CONFIG);

            var ready = tickUntilTimeout(voter);
            assertEquals(Role.PRE_CANDIDATE, voter.role());

            var preVoteTargets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.PreVote)
                .map(Ready.Outbound::to)
                .toList();

            assertFalse(preVoteTargets.contains("n3"));
            assertTrue(preVoteTargets.contains("n2"));
        }

        @Test
        void leaderReplicatesToLearner() {
            var membership = new Membership(Set.of("n1", "n2"), Set.of("n3"));
            var leader = createRaft("n1", membership, CONFIG);
            becomeLeader(leader, List.of("n2"));

            var ready = leader.step(new RaftEvent.Propose("hello".getBytes()));

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(Ready.Outbound::to)
                .toList();

            assertTrue(targets.contains("n2"));
            assertTrue(targets.contains("n3"));
        }
    }

    @Nested
    class ConfigChanges {
        @Test
        void leaderAddsLearner() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n4")));

            var configEntry = findEntry(ready, LogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n4"));
        }

        @Test
        void leaderPromotesLearner() {
            var membership = new Membership(Set.of("n1", "n2"), Set.of("n3"));
            var raft = createRaft("n1", membership, CONFIG);
            becomeLeader(raft, List.of("n2"));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.PromoteVoter("n3")));

            var configEntry = findEntry(ready, LogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isVoter("n3"));
            assertFalse(configEntry.get().membership().isLearner("n3"));
        }

        @Test
        void leaderDemotesVoter() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.DemoteToLearner("n3")));

            var configEntry = findEntry(ready, LogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n3"));
            assertFalse(configEntry.get().membership().isVoter("n3"));
        }

        @Test
        void leaderRemovesNode() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.RemoveNode("n3")));

            var configEntry = findEntry(ready, LogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertFalse(configEntry.get().membership().isMember("n3"));
        }

        @Test
        void nonLeaderIgnoresConfigChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n4")));

            assertTrue(ready.persist().entries().isEmpty());
            assertTrue(ready.messages().isEmpty());
        }

        @Test
        void rejectSecondPendingConfigChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n4")));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n5")));

            assertTrue(findEntry(ready, LogEntry.Config.class).isEmpty());
        }

        @Test
        void singleNodeLeaderCommitsConfigImmediately() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n2")));

            assertEquals(1, ready.apply().membershipChanges().size());
            var change = ready.apply().membershipChanges().getFirst();
            assertFalse(change.previous().isMember("n2"));
            assertTrue(change.current().isLearner("n2"));
        }

        @Test
        void configChangeUpdatesQuorum() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.DemoteToLearner("n3")));

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderStepsDownAfterSelfRemoval() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.RemoveNode("n1")));

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(Role.FOLLOWER, raft.role());
        }

        @Test
        void leaderStepsDownAfterSelfDemotion() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.DemoteToLearner("n1")));

            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(Role.FOLLOWER, raft.role());
        }

        @Test
        void configChangeAppliesOnCommit() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n4")));
            assertFalse(raft.membership().isLearner("n4"));

            var ready = raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertTrue(raft.membership().isLearner("n4"));
            assertEquals(1, ready.apply().membershipChanges().size());
        }

        @Test
        void cannotDemoteLastVoter() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.DemoteToLearner("n1")));

            assertTrue(findEntry(ready, LogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotRemoveLastVoter() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.RemoveNode("n1")));

            assertTrue(findEntry(ready, LogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotAddExistingMember() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n2")));

            assertTrue(findEntry(ready, LogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotPromoteNonLearner() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.PromoteVoter("n4")));

            assertTrue(findEntry(ready, LogEntry.Config.class).isEmpty());
        }

        @Test
        void allowsSecondConfigChangeAfterFirstCommits() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n4")));
            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n5")));

            var configEntry = findEntry(ready, LogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n5"));
        }

        @Test
        void newPeerAddedToReplication() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.AddLearner("n4")));
            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = tickHeartbeat(raft);

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(Ready.Outbound::to)
                .toList();
            assertTrue(targets.contains("n4"));
        }

        @Test
        void removedPeerExcludedFromReplication() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftEvent.ChangeConfig(new ConfigChange.RemoveNode("n3")));
            raft.step(new RaftEvent.Receive("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = tickHeartbeat(raft);

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(Ready.Outbound::to)
                .toList();
            assertFalse(targets.contains("n3"));
        }
    }
}
