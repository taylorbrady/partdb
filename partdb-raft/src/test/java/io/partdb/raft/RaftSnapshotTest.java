package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.AppendRejected;
import io.partdb.raft.Message.HeartbeatResponse;
import io.partdb.raft.Message.SnapshotInstalled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftSnapshotTest {

    private static final Membership MEMBERSHIP = Ids.voters("n1", "n2", "n3");

    private static LogEntry.Command command(long index, long term) {
        return new LogEntry.Command(index, term, Bytes.utf8("v" + index));
    }

    private static TestNode compactedLeader() {
        var log = new InMemoryRaftLog();
        log.append(LongStream.rangeClosed(1, 10).mapToObj(index -> (LogEntry) command(index, 1)).toList());
        log.persistHardState(new HardState(1, Optional.empty(), 10));
        log.compact(new SnapshotMetadata(8, 1, MEMBERSHIP));
        var node = TestNode.restore("n1", MEMBERSHIP, log, 10);
        node.elect("n2", "n3");
        return node;
    }

    @Nested
    class Leader {
        @Test
        void requestsSnapshotForActivePeerBehindCompaction() {
            var node = compactedLeader();

            var output = node.receive("n2", new AppendRejected(2, 10, 3, 1));

            assertEquals(Ids.ids("n2"), output.snapshotRequests());
            assertTrue(output.sentTo("n2", AppendEntries.class).isEmpty());
        }

        @Test
        void waitsForSnapshotResultBeforeSendingMore() {
            var node = compactedLeader();
            node.receive("n2", new AppendRejected(2, 10, 3, 1));

            var output = node.propose(1, "a");

            assertTrue(output.sentTo("n2", AppendEntries.class).isEmpty());
            assertTrue(output.snapshotRequests().isEmpty());
        }

        @Test
        void resumesFromFollowerMatchAfterSnapshotResponse() {
            var node = compactedLeader();
            node.receive("n2", new AppendRejected(2, 10, 3, 1));

            var output = node.receive("n2", new SnapshotInstalled(2, 8));

            var append = output.singleTo("n2", AppendEntries.class);
            assertEquals(8, append.prevLogIndex());
            assertEquals(1, append.prevLogTerm());
        }

        @Test
        void probesAfterSnapshotIsSentOnceFollowerResponds() {
            var node = compactedLeader();
            node.receive("n2", new AppendRejected(2, 10, 3, 1));

            assertTrue(node.step(new Input.SnapshotSent(Ids.id("n2"))).sentTo("n2", AppendEntries.class).isEmpty());

            var output = node.receive("n2", new HeartbeatResponse(2, 0));
            assertEquals(8, output.singleTo("n2", AppendEntries.class).prevLogIndex());
        }

        @Test
        void retriesSnapshotAfterFailure() {
            var node = compactedLeader();
            node.receive("n2", new AppendRejected(2, 10, 3, 1));
            node.step(new Input.SnapshotFailed(Ids.id("n2")));

            var output = node.receive("n2", new HeartbeatResponse(2, 0));

            assertEquals(Ids.ids("n2"), output.snapshotRequests());
        }
    }

    @Nested
    class Follower {
        @Test
        void installsSnapshotAndDiscardsLog() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1), command(2, 1)), 0));
            var snapshot = new SnapshotMetadata(5, 2, MEMBERSHIP);

            var output = node.installSnapshot("n3", 2, snapshot);

            assertEquals(List.of(snapshot), output.snapshots());
            assertEquals(new SnapshotInstalled(2, 5), output.single(SnapshotInstalled.class));
            assertEquals(5, node.raft.commitIndex());
            assertEquals(5, node.raft.appliedIndex());
            assertTrue(node.log.entries().isEmpty());
            assertTrue(output.committed().isEmpty());

            var append = node.receive("n3", new AppendEntries(2, 5, 2, List.of(command(6, 2)), 6));
            assertEquals(List.of(command(6, 2)), append.persisted());
            assertEquals(List.of(command(6, 2)), append.committed());
        }

        @Test
        void advancesCommitInsteadOfInstallingWhenLogMatches() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1), command(2, 1)), 0));

            var output = node.installSnapshot("n2", 1, new SnapshotMetadata(2, 1, MEMBERSHIP));

            assertTrue(output.snapshots().isEmpty());
            assertEquals(2, node.raft.commitIndex());
            assertEquals(List.of(command(1, 1), command(2, 1)), output.committed());
            assertEquals(new SnapshotInstalled(1, 2), output.single(SnapshotInstalled.class));
        }

        @Test
        void ignoresSnapshotAtOrBelowCommit() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(command(1, 1), command(2, 1)), 2));

            var output = node.installSnapshot("n2", 1, new SnapshotMetadata(1, 1, MEMBERSHIP));

            assertTrue(output.snapshots().isEmpty());
            assertEquals(new SnapshotInstalled(1, 2), output.single(SnapshotInstalled.class));
        }

        @Test
        void adoptsSnapshotMembership() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            var membership = Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4"));

            node.installSnapshot("n2", 1, new SnapshotMetadata(3, 1, membership));

            assertEquals(membership, node.raft.membership());
        }

        @Test
        void rejectsSnapshotFromStaleLeader() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.follow("n2", 5);

            var output = node.installSnapshot("n3", 3, new SnapshotMetadata(10, 3, MEMBERSHIP));

            assertTrue(output.snapshots().isEmpty());
            assertEquals(5, output.single(HeartbeatResponse.class).term());
        }
    }
}
