package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeReadIndexTest extends RaftNodeTestSupport {

    @Nested
    class ReadIndexRequested {
        @Test
        void leaderConfirmsReadIndexImmediatelyForSingleNode() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            assertEquals(1, ready.application().readStates().size());
            var readState = ready.application().readStates().get(0);
            assertEquals(raft.commitIndex(), readState.index());
            assertEquals(bytes("req-1"), readState.context());
        }

        @Test
        void leaderBroadcastsHeartbeatForReadIndexConfirmation() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            assertTrue(ready.application().readStates().isEmpty());
            var appendEntries = getAppendEntries(ready);
            assertEquals(2, appendEntries.size());
        }

        @Test
        void leaderConfirmsReadIndexOnQuorumAck() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var ackReady = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            assertEquals(1, ackReady.application().readStates().size());
            assertEquals(bytes("req-1"), ackReady.application().readStates().get(0).context());
        }

        @Test
        void leaderRespondsToForwardedReadIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            long commitIndexAtRequest = raft.commitIndex();
            var readIndexMsg = new RaftMessage.ReadIndexRequested(raft.term(), bytes("req-1"));
            raft.step(new RaftInput.MessageReceived("n2", readIndexMsg));

            var ackReady = raft.step(new RaftInput.MessageReceived("n3",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            var response = findResponse(ackReady, RaftMessage.ReadIndexResponse.class);
            assertTrue(response.isPresent());
            assertEquals(commitIndexAtRequest, response.get().readIndex());
            assertEquals(bytes("req-1"), response.get().context());
        }

        @Test
        void leaderClearsPendingReadsOnBecomeFollower() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var higherTermMsg = new RaftMessage.AppendEntries(10, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));
            assertEquals(RaftRole.FOLLOWER, raft.role());

            var ackReady = raft.step(new RaftInput.MessageReceived("n3",
                new RaftMessage.AppendEntriesResponse(1, true, 1)));

            assertTrue(ackReady.application().readStates().isEmpty());
        }

        @Test
        void followerForwardsReadIndexToLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));
            assertEquals("n2", raft.leaderId().orElseThrow());

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var readIndexMsg = ready.messages().stream()
                .filter(m -> m.to().equals("n2"))
                .filter(m -> m.message() instanceof RaftMessage.ReadIndexRequested)
                .map(m -> (RaftMessage.ReadIndexRequested) m.message())
                .findFirst();

            assertTrue(readIndexMsg.isPresent());
            assertEquals(bytes("req-1"), readIndexMsg.get().context());
        }

        @Test
        void followerEmitsReadStateOnResponse() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 5, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(1, ready.application().readStates().size());
            assertEquals(5, ready.application().readStates().get(0).index());
            assertEquals(bytes("req-1"), ready.application().readStates().get(0).context());
        }

        @Test
        void followerDropsReadIndexWhenNoLeaderKnown() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertTrue(raft.leaderId().isEmpty());

            var ready = raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            assertTrue(ready.messages().isEmpty());
            assertTrue(ready.application().readStates().isEmpty());
        }

        @Test
        void readIndexResponseWithZeroIndexIndicatesFailure() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 0, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n2", response));

            assertTrue(ready.application().readStates().isEmpty());
        }

        @Test
        void nonLeaderRespondsWithZeroReadIndex() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var readIndexMsg = new RaftMessage.ReadIndexRequested(1, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n2", readIndexMsg));

            var response = findResponse(ready, RaftMessage.ReadIndexResponse.class).orElseThrow();
            assertEquals(0, response.readIndex());
        }

        @Test
        void multipleReadIndexRequestsTrackedIndependently() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.ReadIndexRequested(bytes("req-1")));
            raft.step(new RaftInput.ReadIndexRequested(bytes("req-2")));

            var ackReady = raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.AppendEntriesResponse(raft.term(), true, 1)));

            assertEquals(2, ackReady.application().readStates().size());
        }

        @Test
        void readIndexDoesNotTriggerTermChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());

            var readIndexMsg = new RaftMessage.ReadIndexRequested(10, bytes("req-1"));
            raft.step(new RaftInput.MessageReceived("n2", readIndexMsg));

            assertEquals(0, raft.term());
        }

        @Test
        void readIndexResponseDoesNotTriggerTermChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());

            var response = new RaftMessage.ReadIndexResponse(10, 5, bytes("req-1"));
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(0, raft.term());
        }

        @Test
        void followerIgnoresReadIndexResponseFromUnexpectedSender() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));
            assertEquals("n2", raft.leaderId().orElseThrow());

            var response = new RaftMessage.ReadIndexResponse(1, 5, bytes("req-1"));
            var ready = raft.step(new RaftInput.MessageReceived("n3", response));

            assertTrue(ready.application().readStates().isEmpty());
        }
    }
}
