package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftMessage.Heartbeat;
import io.partdb.raft.RaftMessage.RequestVote;
import io.partdb.raft.RaftMessage.RequestVoteResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftRecoveryTest {

    private static final RaftMembership MEMBERSHIP = RaftMembership.voters("n1", "n2", "n3");

    private static RaftLogEntry.Command command(long index, long term) {
        return new RaftLogEntry.Command(index, term, Bytes.utf8("v" + index));
    }

    @Test
    void restoresTermAndVote() {
        var log = new InMemoryRaftLog();
        log.persistHardState(new RaftHardState(4, Optional.of("n2"), 0));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 0);

        assertEquals(4, node.raft.term());
        var output = node.receive("n3", new RequestVote(4, 10, 4, false));
        assertFalse(output.single(RequestVoteResponse.class).granted());
    }

    @Test
    void replaysCommittedEntriesAfterAppliedIndex() {
        var log = new InMemoryRaftLog();
        log.append(List.of(command(1, 1), command(2, 1), command(3, 1)));
        log.persistHardState(new RaftHardState(1, Optional.empty(), 3));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 1);
        var output = node.drain();

        assertEquals(List.of(command(2, 1), command(3, 1)), output.committed());
        assertEquals(3, node.raft.appliedIndex());
    }

    @Test
    void recoversLatestMembershipInTheLog() {
        var log = new InMemoryRaftLog();
        var learner = RaftMembership.of(Set.of("n1", "n2", "n3"), Set.of("n4"));
        var voter = RaftMembership.of(Set.of("n1", "n2", "n3", "n4"), Set.of());
        log.append(List.of(new RaftLogEntry.Membership(1, 1, learner), new RaftLogEntry.Membership(2, 1, voter)));
        log.persistHardState(new RaftHardState(1, Optional.empty(), 1));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 1);

        assertEquals(voter, node.raft.membership());
    }

    @Test
    void startsFromSnapshotWhenLogIsCompacted() {
        var log = new InMemoryRaftLog();
        log.append(List.of(command(1, 1), command(2, 1), command(3, 1)));
        log.persistHardState(new RaftHardState(1, Optional.empty(), 3));
        log.compact(new SnapshotMetadata(2, 1, MEMBERSHIP));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 2);

        assertEquals(List.of(command(3, 1)), node.drain().committed());
    }

    @Test
    void rejectsAppliedIndexOutsideTheLog() {
        var log = new InMemoryRaftLog();
        log.append(List.of(command(1, 1), command(2, 1)));
        log.compact(new SnapshotMetadata(1, 1, MEMBERSHIP));

        assertThrows(IllegalArgumentException.class, () -> TestNode.restore("n1", MEMBERSHIP, log, 0));
        assertThrows(IllegalArgumentException.class, () -> TestNode.restore("n1", MEMBERSHIP, log, 3));
    }

    @Test
    void restoredNodeCanLeadWithItsLog() {
        var log = new InMemoryRaftLog();
        log.append(List.of(command(1, 1), command(2, 1)));
        log.persistHardState(new RaftHardState(1, Optional.empty(), 2));
        var node = TestNode.restore("n1", MEMBERSHIP, log, 2);

        var output = node.elect("n2", "n3");

        assertEquals(2, node.raft.term());
        assertEquals(List.of(new RaftLogEntry.NoOp(3, 2)), output.persisted());
    }

    @Test
    void ignoresMessagesFromOlderTerms() {
        var log = new InMemoryRaftLog();
        log.persistHardState(new RaftHardState(4, Optional.empty(), 0));
        var node = TestNode.restore("n1", MEMBERSHIP, log, 0);

        node.receive("n2", new Heartbeat(3, 0, 0));

        assertEquals(4, node.raft.term());
        assertTrue(node.raft.leaderId().isEmpty());
    }
}
