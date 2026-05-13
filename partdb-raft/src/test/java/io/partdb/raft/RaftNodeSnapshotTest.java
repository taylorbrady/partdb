package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeSnapshotTest extends RaftNodeTestSupport {

    @Nested
    class Snapshots {
        @Test
        void followerInstallsSnapshotFromLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 10, 1, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", snapshot));

            assertNotNull(ready.persistence().incomingSnapshot().orElse(null));
            assertEquals(10, ready.persistence().incomingSnapshot().orElseThrow().index());
        }

        @Test
        void followerRejectsSnapshotBelowCommitIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var entries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                1
            );
            raft.step(new RaftInput.MessageReceived("n2", entries));
            assertEquals(1, raft.commitIndex());

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 1, 1, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            var ready = raft.step(new RaftInput.MessageReceived("n2", snapshot));

            assertNull(ready.persistence().incomingSnapshot().orElse(null));
        }

        @Test
        void leaderSendsSnapshotWhenFollowerBehindCompactedLog() {
            var raft = createRaftWithCompactedSnapshot("n1", 1, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var reject = new RaftMessage.AppendEntriesResponse(1, false, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", reject));

            assertNotNull(ready.snapshotNeeded().orElse(null));
            assertEquals("n2", ready.snapshotNeeded().orElse(null).peer());
        }

        @Test
        void leaderUpdatesIndicesOnInstallSnapshotResponse() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var response = new RaftMessage.InstallSnapshotResponse(raft.term());
            raft.step(new RaftInput.MessageReceived("n2", response));

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
            var snapshotReady = raft.step(new RaftInput.MessageReceived("n2", reject));
            assertNotNull(snapshotReady.snapshotNeeded().orElse(null));
            assertEquals("n2", snapshotReady.snapshotNeeded().orElse(null).peer());

            var oldTermResponse = new RaftMessage.InstallSnapshotResponse(0);
            raft.step(new RaftInput.MessageReceived("n2", oldTermResponse));

            var ready = tickHeartbeat(raft);

            var snapshotToN2 = ready.snapshotNeeded().orElse(null);
            assertNotNull(snapshotToN2);
            assertEquals("n2", snapshotToN2.peer());
        }

        @Test
        void leaderIgnoresInstallSnapshotResponseWhenNotLeader() {
            var raft = createRaftWithCompactedSnapshot("n1", 5, 1, "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var higherTermMsg = new RaftMessage.AppendEntries(10, "n3", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n3", higherTermMsg));
            assertEquals(RaftRole.FOLLOWER, raft.role());

            var response = new RaftMessage.InstallSnapshotResponse(1);
            var ready = raft.step(new RaftInput.MessageReceived("n2", response));

            assertTrue(ready.messages().isEmpty());
        }

        @Test
        void installSnapshotFromLowerTermIsRejected() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));
            assertEquals(5, raft.term());

            var snapshot = new RaftMessage.InstallSnapshot(
                3, "n3", 10, 3, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            var ready = raft.step(new RaftInput.MessageReceived("n3", snapshot));

            assertNull(ready.persistence().incomingSnapshot().orElse(null));

            var response = findResponse(ready, RaftMessage.InstallSnapshotResponse.class).orElseThrow();
            assertEquals(5, response.term());
        }

        @Test
        void installSnapshotResetsLeaderContactTicks() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 5; i++) {
                raft.step(new RaftInput.Tick());
            }

            var snapshot = new RaftMessage.InstallSnapshot(
                1, "n2", 10, 1, RaftMembership.voters("n1", "n2", "n3"), bytes("snapshot-data")
            );
            raft.step(new RaftInput.MessageReceived("n2", snapshot));

            var preVote = new RaftMessage.PreVote(2, "n3", 10, 1);
            var ready = raft.step(new RaftInput.MessageReceived("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }
    }
}
