package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.AppendAccepted;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.HeartbeatResponse;
import io.partdb.raft.Message.ReadIndexRequest;
import io.partdb.raft.Message.ReadIndexResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftReadIndexTest {

    private static Input.ReadIndex read(long id) {
        return new Input.ReadIndex(id);
    }

    @Nested
    class Leader {
        @Test
        void singleVoterConfirmsImmediately() {
            var node = TestNode.create("n1", "n1");
            node.campaign();

            var output = node.step(read(1));

            assertEquals(List.of(new Outcome.ReadReady(1, 1)), output.outcomes());
        }

        @Test
        void startsOneHeartbeatRoundForReadsInTheSameBatch() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");

            var output = node.step(read(1), read(2));

            assertTrue(output.outcomes().isEmpty());
            assertEquals(new Heartbeat(1, 1, 1), output.singleTo("n2", Heartbeat.class));
            assertEquals(new Heartbeat(1, 1, 1), output.singleTo("n3", Heartbeat.class));
        }

        @Test
        void confirmsOnQuorumAckOfCurrentRound() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1), read(2));

            var output = node.receive("n2", new HeartbeatResponse(1, 1));

            assertEquals(List.of(new Outcome.ReadReady(1, 1), new Outcome.ReadReady(2, 1)), output.outcomes());
        }

        @Test
        void ignoresAcksFromEarlierRounds() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1));

            assertTrue(node.receive("n2", new HeartbeatResponse(1, 0)).outcomes().isEmpty());
            assertEquals(1, node.receive("n2", new HeartbeatResponse(1, 1)).outcomes().size());
        }

        @Test
        void readsUseCommitIndexAtRequestTime() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1));
            node.propose(1, "a");
            node.receive("n3", new AppendAccepted(1, 2));

            var output = node.receive("n2", new HeartbeatResponse(1, 1));

            assertEquals(List.of(new Outcome.ReadReady(1, 1)), output.outcomes());
        }

        @Test
        void defersReadsUntilLeaderCommitsInItsTerm() {
            var log = new InMemoryRaftLog();
            log.append(List.of(new LogEntry.Command(1, 1, Bytes.utf8("a")), new LogEntry.Command(2, 1, Bytes.utf8("b"))));
            log.persistHardState(new HardState(1, Optional.empty(), 1));
            var node = TestNode.restore("n1", Ids.voters("n1", "n2", "n3"), log, 0);
            node.elect("n2", "n3");

            var deferred = node.step(read(1));
            assertTrue(deferred.outcomes().isEmpty());
            assertTrue(deferred.sent(Heartbeat.class).isEmpty());

            var committed = node.receive("n2", new AppendAccepted(2, 3));
            assertEquals(3, node.raft.commitIndex());
            assertEquals(1, committed.singleTo("n2", Heartbeat.class).round());

            var confirmed = node.receive("n3", new HeartbeatResponse(2, 1));
            assertEquals(List.of(new Outcome.ReadReady(1, 3)), confirmed.outcomes());
        }

        @Test
        void answersForwardedReads() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.receive("n2", new ReadIndexRequest(1, 9));

            var output = node.receive("n3", new HeartbeatResponse(1, 1));

            assertEquals(new ReadIndexResponse(1, 9, OptionalLong.of(1)), output.singleTo("n2", ReadIndexResponse.class));
        }

        @Test
        void rejectsPendingReadsWhenSteppingDown() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.step(read(1));
            node.receive("n2", new ReadIndexRequest(1, 9));

            var output = node.receive("n2", new Heartbeat(5, 0, 0));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.of(Ids.id("n2"))))), output.outcomes());
            assertEquals(new ReadIndexResponse(1, 9, OptionalLong.empty()), output.singleTo("n2", ReadIndexResponse.class));
        }

        @Test
        void rejectsDeferredReadsWhenSteppingDown() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");
            node.step(read(1));

            var output = node.receive("n2", new Heartbeat(5, 0, 0));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.of(Ids.id("n2"))))), output.outcomes());
        }

        @Test
        void learnersDoNotConfirmReads() {
            var node = TestNode.create("n1", Ids.membership(java.util.Set.of("n1", "n2", "n3"), java.util.Set.of("n4")));
            node.electAndCommit("n2", "n3");
            node.step(read(1));

            assertTrue(node.receive("n4", new HeartbeatResponse(1, 1)).outcomes().isEmpty());
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

            assertEquals(new ReadIndexRequest(1, 1), output.singleTo("n2", ReadIndexRequest.class));
        }

        @Test
        void confirmsReadFromLeaderResponse() {
            var node = follower();
            node.step(read(1));

            var output = node.receive("n2", new ReadIndexResponse(1, 1, OptionalLong.of(5)));

            assertEquals(List.of(new Outcome.ReadReady(1, 5)), output.outcomes());
        }

        @Test
        void reportsLeaderRejection() {
            var node = follower();
            node.step(read(1));

            var output = node.receive("n2", new ReadIndexResponse(1, 1, OptionalLong.empty()));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.empty()))), output.outcomes());
        }

        @Test
        void ignoresResponsesForUnknownReadsOrFromNonLeaders() {
            var node = follower();
            node.step(read(1));

            assertTrue(node.receive("n2", new ReadIndexResponse(1, 7, OptionalLong.of(5))).outcomes().isEmpty());
            assertTrue(node.receive("n3", new ReadIndexResponse(1, 1, OptionalLong.of(5))).outcomes().isEmpty());
        }

        @Test
        void rejectsReadImmediatelyWithoutLeader() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.step(read(1));

            assertTrue(output.messages().isEmpty());
            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.empty()))), output.outcomes());
        }

        @Test
        void rejectsForwardedReadWhenLeaderChanges() {
            var node = follower();
            node.step(read(1));

            var output = node.receive("n3", new AppendEntries(2, 0, 0, List.of(), 0));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.of(Ids.id("n3"))))), output.outcomes());
        }

        @Test
        void expiresForwardedReadWithoutResponse() {
            var node = follower();
            node.step(read(1));

            var rejected = new java.util.ArrayList<Outcome>();
            for (int tick = 0; tick < 2 * TestNode.OPTIONS.electionTicks() && rejected.isEmpty(); tick++) {
                node.follow("n2", 1);
                rejected.addAll(node.tick(1).outcomes());
            }

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.TimedOut())), rejected);
            assertEquals(Optional.of(Ids.id("n2")), node.raft.leaderId());
        }

        @Test
        void nonLeaderRejectsForwardedRead() {
            var node = follower();

            var output = node.receive("n3", new ReadIndexRequest(1, 4));

            assertEquals(new ReadIndexResponse(1, 4, OptionalLong.empty()), output.single(ReadIndexResponse.class));
        }
    }
}
