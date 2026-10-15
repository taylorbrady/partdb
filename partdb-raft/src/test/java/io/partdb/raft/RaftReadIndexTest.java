package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftEffects.ReadResult;
import io.partdb.raft.RaftMessage.AppendAccepted;
import io.partdb.raft.RaftMessage.AppendEntries;
import io.partdb.raft.RaftMessage.Heartbeat;
import io.partdb.raft.RaftMessage.HeartbeatResponse;
import io.partdb.raft.RaftMessage.ReadIndex;
import io.partdb.raft.RaftMessage.ReadIndexResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftReadIndexTest {

    private static RaftInput.ReadIndex read(long id) {
        return new RaftInput.ReadIndex(id);
    }

    @Nested
    class Leader {
        @Test
        void singleVoterConfirmsImmediately() {
            var node = TestNode.create("n1", "n1");
            node.campaign();

            var output = node.step(read(1));

            assertEquals(List.of(new ReadResult.Confirmed(1, 1)), output.reads());
        }

        @Test
        void startsOneHeartbeatRoundForReadsInTheSameBatch() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");

            var output = node.step(read(1), read(2));

            assertTrue(output.reads().isEmpty());
            assertEquals(new Heartbeat(1, 1, 1), output.singleTo("n2", Heartbeat.class));
            assertEquals(new Heartbeat(1, 1, 1), output.singleTo("n3", Heartbeat.class));
        }

        @Test
        void confirmsOnQuorumAckOfCurrentRound() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1), read(2));

            var output = node.receive("n2", new HeartbeatResponse(1, 1));

            assertEquals(List.of(new ReadResult.Confirmed(1, 1), new ReadResult.Confirmed(2, 1)), output.reads());
        }

        @Test
        void ignoresAcksFromEarlierRounds() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1));

            assertTrue(node.receive("n2", new HeartbeatResponse(1, 0)).reads().isEmpty());
            assertEquals(1, node.receive("n2", new HeartbeatResponse(1, 1)).reads().size());
        }

        @Test
        void readsUseCommitIndexAtRequestTime() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1));
            node.propose(1, "a");
            node.receive("n3", new AppendAccepted(1, 2));

            var output = node.receive("n2", new HeartbeatResponse(1, 1));

            assertEquals(List.of(new ReadResult.Confirmed(1, 1)), output.reads());
        }

        @Test
        void defersReadsUntilLeaderCommitsInItsTerm() {
            var log = new InMemoryRaftLog();
            log.append(List.of(new RaftLogEntry.Command(1, 1, Bytes.utf8("a")), new RaftLogEntry.Command(2, 1, Bytes.utf8("b"))));
            log.persistHardState(new RaftHardState(1, Optional.empty(), 1));
            var node = TestNode.restore("n1", RaftMembership.voters("n1", "n2", "n3"), log, 0);
            node.elect("n2", "n3");

            var deferred = node.step(read(1));
            assertTrue(deferred.reads().isEmpty());
            assertTrue(deferred.sent(Heartbeat.class).isEmpty());

            var committed = node.receive("n2", new AppendAccepted(2, 3));
            assertEquals(3, node.raft.commitIndex());
            assertEquals(1, committed.singleTo("n2", Heartbeat.class).round());

            var confirmed = node.receive("n3", new HeartbeatResponse(2, 1));
            assertEquals(List.of(new ReadResult.Confirmed(1, 3)), confirmed.reads());
        }

        @Test
        void answersForwardedReads() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.receive("n2", new ReadIndex(1, 9));

            var output = node.receive("n3", new HeartbeatResponse(1, 1));

            assertEquals(new ReadIndexResponse(1, 9, 1), output.singleTo("n2", ReadIndexResponse.class));
        }

        @Test
        void rejectsPendingReadsWhenSteppingDown() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1));
            node.receive("n2", new ReadIndex(1, 9));

            var output = node.receive("n2", new Heartbeat(5, 0, 0));

            assertEquals(List.of(new ReadResult.Rejected(1)), output.reads());
            assertEquals(new ReadIndexResponse(1, 9, 0), output.singleTo("n2", ReadIndexResponse.class));
        }

        @Test
        void rejectsDeferredReadsWhenSteppingDown() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");
            node.step(read(1));

            var output = node.receive("n2", new Heartbeat(5, 0, 0));

            assertEquals(List.of(new ReadResult.Rejected(1)), output.reads());
        }

        @Test
        void learnersDoNotConfirmReads() {
            var node = TestNode.create("n1", RaftMembership.of(java.util.Set.of("n1", "n2", "n3"), java.util.Set.of("n4")));
            node.electAndCommit("n2", "n3");
            node.step(read(1));

            assertTrue(node.receive("n4", new HeartbeatResponse(1, 1)).reads().isEmpty());
        }
    }

    @Nested
    class Follower {
        private TestNode follower() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.follow("n2", 1);
            return node;
        }

        @Test
        void forwardsReadToLeader() {
            var node = follower();

            var output = node.step(read(1));

            assertEquals(new ReadIndex(1, 1), output.singleTo("n2", ReadIndex.class));
        }

        @Test
        void confirmsReadFromLeaderResponse() {
            var node = follower();
            node.step(read(1));

            var output = node.receive("n2", new ReadIndexResponse(1, 1, 5));

            assertEquals(List.of(new ReadResult.Confirmed(1, 5)), output.reads());
        }

        @Test
        void reportsLeaderRejection() {
            var node = follower();
            node.step(read(1));

            var output = node.receive("n2", new ReadIndexResponse(1, 1, 0));

            assertEquals(List.of(new ReadResult.Rejected(1)), output.reads());
        }

        @Test
        void ignoresResponsesForUnknownReadsOrFromNonLeaders() {
            var node = follower();
            node.step(read(1));

            assertTrue(node.receive("n2", new ReadIndexResponse(1, 7, 5)).reads().isEmpty());
            assertTrue(node.receive("n3", new ReadIndexResponse(1, 1, 5)).reads().isEmpty());
        }

        @Test
        void rejectsReadImmediatelyWithoutLeader() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.step(read(1));

            assertTrue(output.messages().isEmpty());
            assertEquals(List.of(new ReadResult.Rejected(1)), output.reads());
        }

        @Test
        void rejectsForwardedReadWhenLeaderChanges() {
            var node = follower();
            node.step(read(1));

            var output = node.receive("n3", new AppendEntries(2, 0, 0, List.of(), 0));

            assertEquals(List.of(new ReadResult.Rejected(1)), output.reads());
        }

        @Test
        void expiresForwardedReadWithoutResponse() {
            var node = follower();
            node.step(read(1));

            var rejected = new java.util.ArrayList<ReadResult>();
            for (int tick = 0; tick < 2 * TestNode.OPTIONS.electionTicks() && rejected.isEmpty(); tick++) {
                node.follow("n2", 1);
                rejected.addAll(node.tick(1).reads());
            }

            assertEquals(List.of(new ReadResult.Rejected(1)), rejected);
            assertEquals(Optional.of("n2"), node.raft.leaderId());
        }

        @Test
        void nonLeaderRejectsForwardedRead() {
            var node = follower();

            var output = node.receive("n3", new ReadIndex(1, 4));

            assertEquals(new ReadIndexResponse(1, 4, 0), output.single(ReadIndexResponse.class));
        }
    }
}
