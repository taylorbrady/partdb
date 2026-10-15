package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.AppendAccepted;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.AppendRejected;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.HeartbeatResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftReplicationTest {

    private static LogEntry.Command command(long index, long term, String value) {
        return new LogEntry.Command(index, term, Bytes.utf8(value));
    }

    @Nested
    class Follower {
        @Test
        void acceptsMatchingEntriesAndPersistsThem() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a")), 0));

            assertEquals(List.of(command(1, 1, "a")), output.persisted());
            assertEquals(new AppendAccepted(1, 1), output.single(AppendAccepted.class));
        }

        @Test
        void commitsUpToLastNewEntry() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a")), 5));

            assertEquals(1, node.raft.commitIndex());
            assertEquals(List.of(command(1, 1, "a")), output.committed());
        }

        @Test
        void neverMovesCommitBackwards() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")), 3));

            var output = node.receive("n2", new AppendEntries(1, 3, 1, List.of(), 4));

            assertEquals(3, node.raft.commitIndex());
            assertEquals(new AppendAccepted(1, 3), output.single(AppendAccepted.class));
        }

        @Test
        void acknowledgesEntriesBelowCommitWithoutChecking() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a"), command(2, 1, "b")), 2));

            var output = node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a")), 2));

            assertEquals(new AppendAccepted(1, 2), output.single(AppendAccepted.class));
            assertTrue(output.persisted().isEmpty());
        }

        @Test
        void rejectsMissingPreviousEntryWithHint() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a")), 0));

            var output = node.receive("n2", new AppendEntries(1, 5, 1, List.of(command(6, 1, "f")), 0));

            assertEquals(new AppendRejected(1, 5, 1, 1), output.single(AppendRejected.class));
        }

        @Test
        void rejectsConflictingTermWithHintBeforeConflictingTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a")), 1));
            node.receive("n2", new AppendEntries(2, 1, 1, List.of(command(2, 2, "b"), command(3, 2, "c"), command(4, 2, "d")), 1));

            var output = node.receive("n3", new AppendEntries(3, 4, 3, List.of(), 1));

            assertEquals(new AppendRejected(3, 4, 4, 2), output.single(AppendRejected.class));
        }

        @Test
        void truncatesConflictingSuffix() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")), 1));

            var output = node.receive("n3", new AppendEntries(2, 1, 1, List.of(command(2, 2, "x")), 2));

            assertEquals(List.of(command(2, 2, "x")), output.persisted());
            assertEquals(List.of(command(1, 1, "a"), command(2, 2, "x")), node.log.entries());
            assertEquals(List.of(command(2, 2, "x")), output.committed());
        }

        @Test
        void ignoresDuplicateEntries() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            var append = new AppendEntries(1, 0, 0, List.of(command(1, 1, "a"), command(2, 1, "b")), 0);
            node.receive("n2", append);

            var output = node.receive("n2", append);

            assertTrue(output.persisted().isEmpty());
            assertEquals(new AppendAccepted(1, 2), output.single(AppendAccepted.class));
        }

        @Test
        void stalePrefixDoesNotTruncateNewerEntries() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a"), command(2, 1, "b")), 0));

            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a")), 0));

            assertEquals(2, node.raft.lastIndex());
        }

        @Test
        void heartbeatAdvancesCommitWithinLocalLog() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1, "a"), command(2, 1, "b")), 0));

            var output = node.receive("n2", new Heartbeat(1, 7, 4));

            assertEquals(2, node.raft.commitIndex());
            assertEquals(new HeartbeatResponse(1, 4), output.single(HeartbeatResponse.class));
        }
    }

    @Nested
    class Leader {
        @Test
        void proposalIsAcceptedWithItsPosition() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");

            var output = node.propose(7, "a");

            assertEquals(List.of(new Outcome.Appended(7, 2, 1)), output.outcomes());
            assertEquals(List.of(command(2, 1, "a")), output.persisted());
        }

        @Test
        void followerRejectsProposals() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.propose(7, "a");

            assertEquals(List.of(new Outcome.Rejected(7, new Rejection.NotLeader(Optional.empty()))), output.outcomes());
            assertTrue(output.persisted().isEmpty());
        }

        @Test
        void batchesProposalsIntoOneAppendPerPeer() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");

            var output = node.step(
                new Input.Propose(1, Bytes.utf8("a")),
                new Input.Propose(2, Bytes.utf8("b")),
                new Input.Propose(3, Bytes.utf8("c"))
            );

            var append = output.singleTo("n2", AppendEntries.class);
            assertEquals(List.of(command(2, 1, "a"), command(3, 1, "b"), command(4, 1, "c")), append.entries());
            assertEquals(1, append.prevLogIndex());
        }

        @Test
        void commitsOnceQuorumPersists() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.propose(1, "a");

            var output = node.receive("n3", new AppendAccepted(1, 2));

            assertEquals(2, node.raft.commitIndex());
            assertEquals(List.of(command(2, 1, "a")), output.committed());
            assertEquals(2, output.singleTo("n2", AppendEntries.class).commitIndex());
        }

        @Test
        void singleVoterCommitsAfterPersisting() {
            var node = TestNode.create("n1", "n1");
            node.campaign();

            var output = node.propose(1, "a");

            assertEquals(List.of(command(2, 1, "a")), output.committed());
        }

        @Test
        void doesNotCommitPreviousTermEntriesByCounting() {
            var log = new InMemoryRaftLog();
            log.append(List.of(command(1, 1, "a"), command(2, 1, "b")));
            log.persistHardState(new HardState(1, Optional.empty(), 0));
            var node = TestNode.restore("n1", Ids.voters("n1", "n2", "n3", "n4", "n5"), log, 0);
            node.elect("n2", "n3", "n4", "n5");

            node.receive("n2", new AppendAccepted(node.raft.term(), 2));
            node.receive("n3", new AppendAccepted(node.raft.term(), 2));
            assertEquals(0, node.raft.commitIndex());

            node.receive("n2", new AppendAccepted(node.raft.term(), 3));
            node.receive("n3", new AppendAccepted(node.raft.term(), 3));
            assertEquals(3, node.raft.commitIndex());
        }

        @Test
        void replicatesOptimisticallyAfterFirstMatch() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");

            var first = node.propose(1, "a");
            var second = node.propose(2, "b");

            assertEquals(List.of(command(2, 1, "a")), first.singleTo("n2", AppendEntries.class).entries());
            assertEquals(List.of(command(3, 1, "b")), second.singleTo("n2", AppendEntries.class).entries());
            assertEquals(2, second.singleTo("n2", AppendEntries.class).prevLogIndex());
        }

        @Test
        void probesOneAppendAtATimeUntilFollowerMatches() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");

            var output = node.propose(1, "a");

            assertTrue(output.sentTo("n2", AppendEntries.class).isEmpty());
        }

        @Test
        void pausesReplicationWhenInflightLimitIsReached() {
            var options = new Options(10, 3, 1 << 20, 2, 64 << 20);
            var node = TestNode.create("n1", Ids.voters("n1", "n2", "n3"), options);
            node.tick(options.electionTicks());
            node.receive("n2", new Message.PreVoteResponse(1, true));
            node.receive("n2", new Message.RequestVoteResponse(1, true));
            node.receive("n2", new AppendAccepted(1, 1));

            assertEquals(1, node.propose(1, "a").sentTo("n2", AppendEntries.class).size());
            assertEquals(1, node.propose(2, "b").sentTo("n2", AppendEntries.class).size());
            assertTrue(node.propose(3, "c").sentTo("n2", AppendEntries.class).isEmpty());

            var resumed = node.receive("n2", new AppendAccepted(1, 2));
            assertEquals(List.of(command(4, 1, "c")), resumed.singleTo("n2", AppendEntries.class).entries());
        }

        @Test
        void rejectionJumpsBackUsingFollowerHint() {
            var log = new InMemoryRaftLog();
            log.append(LongStream.rangeClosed(1, 10).mapToObj(index -> (LogEntry) command(index, 1, "v" + index)).toList());
            log.persistHardState(new HardState(1, Optional.empty(), 0));
            var node = TestNode.restore("n1", Ids.voters("n1", "n2", "n3"), log, 0);
            node.elect("n2", "n3");

            var output = node.receive("n2", new AppendRejected(node.raft.term(), 10, 3, 1));

            var retry = output.singleTo("n2", AppendEntries.class);
            assertEquals(3, retry.prevLogIndex());
            assertEquals(1, retry.prevLogTerm());
        }

        @Test
        void ignoresStaleRejection() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");

            var output = node.receive("n2", new AppendRejected(1, 0, 0, 0));

            assertTrue(output.sentTo("n2", AppendEntries.class).isEmpty());
        }

        @Test
        void heartbeatResponseResendsWhenFollowerIsBehind() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.propose(1, "a");

            var output = node.receive("n2", new HeartbeatResponse(1, 0));

            assertEquals(1, output.sentTo("n2", AppendEntries.class).size());
        }

        @Test
        void sendsHeartbeatsWithCommitCappedAtMatch() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2");

            var output = node.tick(TestNode.OPTIONS.heartbeatTicks());

            assertEquals(new Heartbeat(1, 1, 0), output.singleTo("n2", Heartbeat.class));
            assertEquals(new Heartbeat(1, 0, 0), output.singleTo("n3", Heartbeat.class));
        }

        @Test
        void unreachablePeerFallsBackToProbing() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.electAndCommit("n2", "n3");
            node.propose(1, "a");

            var output = node.step(new Input.Unreachable(Ids.id("n2")));

            var probe = output.singleTo("n2", AppendEntries.class);
            assertEquals(1, probe.prevLogIndex());
            assertEquals(List.of(command(2, 1, "a")), probe.entries());
            assertTrue(node.propose(2, "b").sentTo("n2", AppendEntries.class).isEmpty());
        }
    }
}
