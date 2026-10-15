package io.partdb.raft;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeReadIndexTest extends RaftNodeTestSupport {

    private static RaftEffects acknowledge(RaftNode raft, String peer, long matchIndex, long round) {
        return raft.step(new RaftInput.MessageReceived(peer,
            new RaftMessage.AppendEntriesResponse(raft.term(), true, matchIndex, round)));
    }

    private RaftNode committedLeader() {
        var raft = createRaft("n1", "n1", "n2", "n3");
        becomeLeader(raft, List.of("n2", "n3"));
        acknowledge(raft, "n2", 1, 0);
        assertEquals(1, raft.commitIndex());
        return raft;
    }

    private RaftNode followerOf(String leader) {
        var raft = createRaft("n1", "n1", "n2", "n3");
        raft.step(new RaftInput.MessageReceived(leader, new RaftMessage.AppendEntries(1, leader, 0, 0, List.of(), 0, 0)));
        assertEquals(leader, raft.leaderId().orElseThrow());
        return raft;
    }

    @Nested
    class Leader {
        @Test
        void confirmsImmediatelyForSingleNode() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.ReadIndexRequested(1L));

            assertEquals(List.of(new RaftEffects.ReadState(raft.commitIndex(), 1L)), ready.application().readStates());
        }

        @Test
        void broadcastsHeartbeatWithNewRound() {
            var raft = committedLeader();

            var ready = raft.step(new RaftInput.ReadIndexRequested(1L));

            assertTrue(ready.application().readStates().isEmpty());
            var appends = getAppendEntries(ready);
            assertEquals(2, appends.size());
            assertTrue(appends.stream().allMatch(append -> append.round() == 1));
        }

        @Test
        void confirmsOnQuorumAckForCurrentRound() {
            var raft = committedLeader();
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = acknowledge(raft, "n2", 1, 1);

            assertEquals(List.of(new RaftEffects.ReadState(1, 1L)), ready.application().readStates());
        }

        @Test
        void ignoresAcksFromEarlierRounds() {
            var raft = committedLeader();
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var stale = acknowledge(raft, "n2", 1, 0);
            assertTrue(stale.application().readStates().isEmpty());

            var current = acknowledge(raft, "n2", 1, 1);
            assertEquals(1, current.application().readStates().size());
        }

        @Test
        void countsRejectedAppendsAsLeadershipAcks() {
            var raft = committedLeader();
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), false, 0, 1)));

            assertEquals(1, ready.application().readStates().size());
        }

        @Test
        void defersReadsUntilCommittedInCurrentTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));
            assertEquals(0, raft.commitIndex());

            var deferred = raft.step(new RaftInput.ReadIndexRequested(1L));
            assertTrue(deferred.application().readStates().isEmpty());
            assertTrue(getAppendEntries(deferred).isEmpty());

            var committed = acknowledge(raft, "n2", 1, 0);
            assertEquals(1, raft.commitIndex());
            assertTrue(committed.application().readStates().isEmpty());
            assertTrue(getAppendEntries(committed).stream().allMatch(append -> append.round() == 1));

            var confirmed = acknowledge(raft, "n3", 1, 1);
            assertEquals(List.of(new RaftEffects.ReadState(1, 1L)), confirmed.application().readStates());
        }

        @Test
        void readIndexIncludesEntriesFromPreviousTerms() {
            var entries = List.<RaftLogEntry>of(
                new RaftLogEntry.Data(1, 1, bytes("a")),
                new RaftLogEntry.Data(2, 1, bytes("b")),
                new RaftLogEntry.Data(3, 1, bytes("c"))
            );
            var raft = createRaftWithLog("n1", entries, new RaftHardState(1, null, 1), "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(1L));
            acknowledge(raft, "n2", 4, 0);
            assertEquals(4, raft.commitIndex());

            var confirmed = acknowledge(raft, "n2", 4, 1);
            assertEquals(List.of(new RaftEffects.ReadState(4, 1L)), confirmed.application().readStates());
        }

        @Test
        void respondsToForwardedReadIndex() {
            var raft = committedLeader();

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexRequested(raft.term(), 1L)));
            var ready = acknowledge(raft, "n3", 1, 1);

            var response = findResponse(ready, RaftMessage.ReadIndexResponse.class).orElseThrow();
            assertEquals(1, response.readIndex());
            assertEquals(1L, response.context());
        }

        @Test
        void failsPendingReadsWhenSteppingDown() {
            var raft = committedLeader();
            raft.step(new RaftInput.ReadIndexRequested(1L));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexRequested(raft.term(), 2L)));

            var ready = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntries(10, "n2", 0, 0, List.of(), 0, 0)));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals(List.of(new RaftEffects.ReadState(0, 1L)), ready.application().readStates());
            var response = findResponse(ready, RaftMessage.ReadIndexResponse.class).orElseThrow();
            assertEquals(0, response.readIndex());
            assertEquals(2L, response.context());
        }

        @Test
        void failsDeferredReadsWhenSteppingDown() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntries(10, "n2", 0, 0, List.of(), 0, 0)));

            assertEquals(List.of(new RaftEffects.ReadState(0, 1L)), ready.application().readStates());
        }

        @Test
        void tracksMultipleReadsIndependently() {
            var raft = committedLeader();
            raft.step(new RaftInput.ReadIndexRequested(1L));
            raft.step(new RaftInput.ReadIndexRequested(2L));

            var first = acknowledge(raft, "n2", 1, 1);
            assertEquals(List.of(new RaftEffects.ReadState(1, 1L)), first.application().readStates());

            var second = acknowledge(raft, "n2", 1, 2);
            assertEquals(List.of(new RaftEffects.ReadState(1, 2L)), second.application().readStates());
        }
    }

    @Nested
    class Follower {
        @Test
        void forwardsReadIndexToLeader() {
            var raft = followerOf("n2");

            var ready = raft.step(new RaftInput.ReadIndexRequested(1L));

            var forwarded = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .map(RaftEffects.Outbound::message)
                .filter(RaftMessage.ReadIndexRequested.class::isInstance)
                .map(RaftMessage.ReadIndexRequested.class::cast)
                .findFirst()
                .orElseThrow();
            assertEquals(1L, forwarded.context());
        }

        @Test
        void emitsReadStateOnResponse() {
            var raft = followerOf("n2");
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexResponse(1, 5, 1L)));

            assertEquals(List.of(new RaftEffects.ReadState(5, 1L)), ready.application().readStates());
        }

        @Test
        void reportsRejectedReadAsFailure() {
            var raft = followerOf("n2");
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexResponse(1, 0, 1L)));

            assertEquals(List.of(new RaftEffects.ReadState(0, 1L)), ready.application().readStates());
        }

        @Test
        void ignoresResponsesForUnknownReads() {
            var raft = followerOf("n2");

            var ready = raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexResponse(1, 5, 1L)));

            assertTrue(ready.application().readStates().isEmpty());
        }

        @Test
        void ignoresResponsesFromNonLeaders() {
            var raft = followerOf("n2");
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = raft.step(new RaftInput.MessageReceived("n3", new RaftMessage.ReadIndexResponse(1, 5, 1L)));

            assertTrue(ready.application().readStates().isEmpty());
        }

        @Test
        void failsReadImmediatelyWhenNoLeaderKnown() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.ReadIndexRequested(1L));

            assertTrue(ready.messages().isEmpty());
            assertEquals(List.of(new RaftEffects.ReadState(0, 1L)), ready.application().readStates());
        }

        @Test
        void failsForwardedReadWhenLeaderChanges() {
            var raft = followerOf("n2");
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var ready = raft.step(new RaftInput.MessageReceived("n3",
                new RaftMessage.AppendEntries(2, "n3", 0, 0, List.of(), 0, 0)));

            assertEquals(List.of(new RaftEffects.ReadState(0, 1L)), ready.application().readStates());
        }

        @Test
        void expiresForwardedReadWithoutResponse() {
            var raft = followerOf("n2");
            raft.step(new RaftInput.ReadIndexRequested(1L));

            var failed = new java.util.ArrayList<RaftEffects.ReadState>();
            for (int tick = 0; tick < CONFIG.electionTimeoutMax() && failed.isEmpty(); tick++) {
                raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0, 0)));
                failed.addAll(raft.step(new RaftInput.Tick()).application().readStates());
            }

            assertEquals(List.of(new RaftEffects.ReadState(0, 1L)), failed);
            assertEquals("n2", raft.leaderId().orElseThrow());
        }

        @Test
        void nonLeaderRejectsForwardedRead() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexRequested(1, 1L)));

            assertEquals(0, findResponse(ready, RaftMessage.ReadIndexResponse.class).orElseThrow().readIndex());
        }

        @Test
        void readIndexMessagesDoNotChangeTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexRequested(10, 1L)));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.ReadIndexResponse(10, 5, 1L)));

            assertEquals(0, raft.term());
        }
    }
}
