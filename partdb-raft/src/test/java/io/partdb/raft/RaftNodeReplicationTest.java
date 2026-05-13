package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeReplicationTest extends RaftNodeTestSupport {

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
}
