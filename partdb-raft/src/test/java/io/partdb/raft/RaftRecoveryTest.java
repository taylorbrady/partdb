package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.RequestVote;
import io.partdb.raft.Message.RequestVoteResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftRecoveryTest {

    private static final Membership MEMBERSHIP = Ids.voters("n1", "n2", "n3");

    private static LogEntry.Command command(long index, long term) {
        return new LogEntry.Command(index, term, Bytes.utf8("v" + index));
    }

    @Test
    void restoresTermAndVote() {
        var log = new InMemoryRaftLog();
        log.persistHardState(new HardState(4, Optional.of(Ids.id("n2")), 0));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 0);

        assertEquals(4, node.raft.term());
        var output = node.receive("n3", new RequestVote(4, 10, 4, false));
        assertFalse(output.single(RequestVoteResponse.class).granted());
    }

    @Test
    void replaysCommittedEntriesAfterAppliedIndex() {
        var log = new InMemoryRaftLog();
        log.append(List.of(command(1, 1), command(2, 1), command(3, 1)));
        log.persistHardState(new HardState(1, Optional.empty(), 3));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 1);
        var output = node.drain();

        assertEquals(List.of(command(2, 1), command(3, 1)), output.committed());
        assertEquals(3, node.raft.appliedIndex());
    }

    @Test
    void recoversLatestMembershipInTheLog() {
        var log = new InMemoryRaftLog();
        var learner = Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4"));
        var voter = Ids.membership(Set.of("n1", "n2", "n3", "n4"), Set.of());
        log.append(List.of(new LogEntry.Configuration(1, 1, learner), new LogEntry.Configuration(2, 1, voter)));
        log.persistHardState(new HardState(1, Optional.empty(), 1));

        var node = TestNode.restore("n1", MEMBERSHIP, log, 1);

        assertEquals(voter, node.raft.membership());
    }

    @Test
    void startsFromSnapshotWhenLogIsCompacted() {
        var log = new InMemoryRaftLog();
        log.append(List.of(command(1, 1), command(2, 1), command(3, 1)));
        log.persistHardState(new HardState(1, Optional.empty(), 3));
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
        log.persistHardState(new HardState(1, Optional.empty(), 2));
        var node = TestNode.restore("n1", MEMBERSHIP, log, 2);

        var output = node.elect("n2", "n3");

        assertEquals(2, node.raft.term());
        assertEquals(List.of(new LogEntry.NoOp(3, 2)), output.persisted());
    }

    @Test
    void ignoresMessagesFromOlderTerms() {
        var log = new InMemoryRaftLog();
        log.persistHardState(new HardState(4, Optional.empty(), 0));
        var node = TestNode.restore("n1", MEMBERSHIP, log, 0);

        node.receive("n2", new Heartbeat(3, 0, 0));

        assertEquals(4, node.raft.term());
        assertTrue(node.raft.leaderId().isEmpty());
    }
}
