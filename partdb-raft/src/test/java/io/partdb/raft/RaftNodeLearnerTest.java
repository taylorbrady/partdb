package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeLearnerTest extends RaftNodeTestSupport {

    @Nested
    class Learners {
        private RaftNode createLearner(String id, Set<String> voters, Set<String> learners) {
            var configuration = new RaftMembership(voters, learners);
            var storage = new InMemoryRaftLog(configuration);
            return RaftNode.builder(id, configuration, CONFIG, storage)
                .electionJitter(_ -> DETERMINISTIC_JITTER)
                .build();
        }

        @Test
        void learnerDoesNotStartElection() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            for (int i = 0; i < 50; i++) {
                learner.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.FOLLOWER, learner.role());
            assertEquals(0, learner.term());
        }

        @Test
        void learnerReceivesAppendEntries() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var request = new RaftMessage.AppendEntries(
                1, "n1", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            var ready = learner.step(new RaftInput.MessageReceived("n1", request));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertTrue(response.success());
            assertEquals(1, response.matchIndex());
        }

        @Test
        void learnerAdvancesCommitIndex() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var request = new RaftMessage.AppendEntries(
                1, "n1", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                1
            );
            learner.step(new RaftInput.MessageReceived("n1", request));

            assertEquals(1, learner.commitIndex());
        }

        @Test
        void learnerDoesNotCountInQuorum() {
            var configuration = new RaftMembership(Set.of("n1", "n2", "n3"), Set.of("n4"));
            var leader = createRaft("n1", configuration, CONFIG);
            becomeLeader(leader, List.of("n2", "n3"));

            leader.step(new RaftInput.EntryProposed(bytes("hello")));
            assertEquals(0, leader.commitIndex());

            var learnerResponse = new RaftMessage.AppendEntriesResponse(1, true, 2);
            leader.step(new RaftInput.MessageReceived("n4", learnerResponse));
            assertEquals(0, leader.commitIndex());

            var voterResponse = new RaftMessage.AppendEntriesResponse(1, true, 2);
            leader.step(new RaftInput.MessageReceived("n2", voterResponse));
            assertEquals(2, leader.commitIndex());
        }

        @Test
        void learnerCanForwardReadIndex() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var heartbeat = new RaftMessage.AppendEntries(1, "n1", 0, 0, List.of(), 0);
            learner.step(new RaftInput.MessageReceived("n1", heartbeat));
            assertEquals("n1", learner.leaderId().orElseThrow());

            var ready = learner.step(new RaftInput.ReadIndexRequested(bytes("req-1")));

            var readIndexMsg = ready.messages().stream()
                .filter(m -> m.to().equals("n1"))
                .filter(m -> m.message() instanceof RaftMessage.ReadIndexRequested)
                .map(m -> (RaftMessage.ReadIndexRequested) m.message())
                .findFirst();

            assertTrue(readIndexMsg.isPresent());
            assertEquals(bytes("req-1"), readIndexMsg.get().context());
        }

        @Test
        void learnerReceivesReadIndexResponse() {
            var learner = createLearner("n3", Set.of("n1", "n2"), Set.of("n3"));

            var heartbeat = new RaftMessage.AppendEntries(1, "n1", 0, 0, List.of(), 0);
            learner.step(new RaftInput.MessageReceived("n1", heartbeat));

            var response = new RaftMessage.ReadIndexResponse(1, 5, bytes("req-1"));
            var ready = learner.step(new RaftInput.MessageReceived("n1", response));

            assertEquals(1, ready.application().readStates().size());
            assertEquals(5, ready.application().readStates().get(0).index());
            assertEquals(bytes("req-1"), ready.application().readStates().get(0).context());
        }

        @Test
        void learnerDoesNotReceiveVoteRequests() {
            var configuration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
            var voter = createRaft("n1", configuration, CONFIG);

            var ready = tickUntilTimeout(voter);
            assertEquals(RaftRole.PRE_CANDIDATE, voter.role());

            var preVoteTargets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.PreVote)
                .map(RaftEffects.Outbound::to)
                .toList();

            assertFalse(preVoteTargets.contains("n3"));
            assertTrue(preVoteTargets.contains("n2"));
        }

        @Test
        void leaderReplicatesToLearner() {
            var configuration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
            var leader = createRaft("n1", configuration, CONFIG);
            becomeLeader(leader, List.of("n2"));

            var ready = leader.step(new RaftInput.EntryProposed(bytes("hello")));

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(RaftEffects.Outbound::to)
                .toList();

            assertTrue(targets.contains("n2"));
            assertTrue(targets.contains("n3"));
        }
    }
}
