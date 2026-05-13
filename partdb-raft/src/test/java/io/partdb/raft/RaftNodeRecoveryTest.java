package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeRecoveryTest extends RaftNodeTestSupport {

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
}
