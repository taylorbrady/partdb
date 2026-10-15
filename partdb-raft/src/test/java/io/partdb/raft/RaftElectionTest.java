package io.partdb.raft;

import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.HeartbeatResponse;
import io.partdb.raft.Message.PreVote;
import io.partdb.raft.Message.PreVoteResponse;
import io.partdb.raft.Message.RequestVote;
import io.partdb.raft.Message.RequestVoteResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftElectionTest {

    @Nested
    class InitialState {
        @Test
        void startsAsLeaderlessFollowerAtTermZero() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            assertEquals(Role.FOLLOWER, node.raft.role());
            assertEquals(0, node.raft.term());
            assertTrue(node.raft.leaderId().isEmpty());
            assertEquals(0, node.raft.commitIndex());
        }
    }

    @Nested
    class PreVoting {
        @Test
        void followerStartsPreVoteAfterElectionTimeout() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            node.tick(TestNode.OPTIONS.electionTicks() - 1);
            assertEquals(Role.FOLLOWER, node.raft.role());

            var output = node.tick(1);

            assertEquals(Role.PRE_CANDIDATE, node.raft.role());
            assertEquals(0, node.raft.term());
            assertEquals(new PreVote(1, 0, 0), output.singleTo("n2", PreVote.class));
            assertEquals(new PreVote(1, 0, 0), output.singleTo("n3", PreVote.class));
            assertTrue(output.hardStates().isEmpty());
        }

        @Test
        void preCandidateCampaignsAfterQuorumOfPreVotes() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.campaign();

            var output = node.receive("n2", new PreVoteResponse(1, true));

            assertEquals(Role.CANDIDATE, node.raft.role());
            assertEquals(1, node.raft.term());
            assertEquals(new RequestVote(1, 0, 0, false), output.singleTo("n2", RequestVote.class));
            assertEquals(Optional.of(Ids.id("n1")), output.hardStates().getLast().votedFor());
        }

        @Test
        void preCandidateStepsDownAfterQuorumOfRejections() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.campaign();

            node.receive("n2", new PreVoteResponse(0, false));
            node.receive("n3", new PreVoteResponse(0, false));

            assertEquals(Role.FOLLOWER, node.raft.role());
            assertEquals(0, node.raft.term());
        }

        @Test
        void grantsPreVoteWithoutChangingTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.receive("n2", new PreVote(5, 0, 0));

            assertEquals(new PreVoteResponse(5, true), output.single(PreVoteResponse.class));
            assertEquals(0, node.raft.term());
            assertTrue(output.hardStates().isEmpty());
        }

        @Test
        void rejectsPreVoteWhileLeaderIsActive() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.follow("n2", 1);

            var output = node.receive("n3", new PreVote(2, 10, 1));

            assertEquals(new PreVoteResponse(1, false), output.single(PreVoteResponse.class));
            assertEquals(1, node.raft.term());
        }

        @Test
        void rejectsPreVoteFromCandidateWithStaleLog() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(1, 0, 0, List.of(new LogEntry.NoOp(1, 1)), 0));
            node.tick(2 * TestNode.OPTIONS.electionTicks());

            var output = node.receive("n3", new PreVote(2, 0, 0));

            assertFalse(output.single(PreVoteResponse.class).granted());
        }

        @Test
        void ignoresGrantedPreVoteFromEarlierRound() {
            var node = TestNode.create("n1", "n1", "n2", "n3", "n4", "n5");
            node.campaign();

            node.receive("n2", new PreVoteResponse(7, true));
            node.receive("n3", new PreVoteResponse(7, true));

            assertEquals(Role.PRE_CANDIDATE, node.raft.role());
        }
    }

    @Nested
    class Voting {
        @Test
        void grantsVoteToFirstUpToDateCandidateAndPersistsIt() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.receive("n2", new RequestVote(1, 0, 0, false));

            assertEquals(new RequestVoteResponse(1, true), output.single(RequestVoteResponse.class));
            assertEquals(new HardState(1, Optional.of(Ids.id("n2")), 0), output.hardStates().getLast());
        }

        @Test
        void rejectsSecondCandidateInSameTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new RequestVote(1, 0, 0, false));

            var output = node.receive("n3", new RequestVote(1, 0, 0, false));

            assertFalse(output.single(RequestVoteResponse.class).granted());
        }

        @Test
        void regrantsVoteToSameCandidate() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new RequestVote(1, 0, 0, false));

            var output = node.receive("n2", new RequestVote(1, 0, 0, false));

            assertTrue(output.single(RequestVoteResponse.class).granted());
        }

        @Test
        void rejectsCandidateWithStaleLog() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new AppendEntries(2, 0, 0, List.of(new LogEntry.NoOp(1, 2)), 0));
            node.tick(2 * TestNode.OPTIONS.electionTicks());

            var output = node.receive("n3", new RequestVote(3, 5, 1, false));

            assertFalse(output.single(RequestVoteResponse.class).granted());
            assertEquals(3, node.raft.term());
        }

        @Test
        void ignoresVoteRequestWhileLeaderIsActive() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.follow("n2", 1);

            var output = node.receive("n3", new RequestVote(2, 0, 0, false));

            assertTrue(output.messages().isEmpty());
            assertEquals(1, node.raft.term());
        }

        @Test
        void leadershipTransferVoteBypassesLease() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.follow("n2", 1);

            var output = node.receive("n3", new RequestVote(2, 0, 0, true));

            assertTrue(output.single(RequestVoteResponse.class).granted());
            assertEquals(2, node.raft.term());
        }

        @Test
        void rejectsVoteFromLowerTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new RequestVote(3, 0, 0, false));

            var output = node.receive("n3", new RequestVote(2, 0, 0, false));

            assertTrue(output.messages().isEmpty());
            assertEquals(3, node.raft.term());
        }
    }

    @Nested
    class Leadership {
        @Test
        void candidateWinsWithQuorumAndAppendsNoOp() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.elect("n2", "n3");

            assertEquals(Role.LEADER, node.raft.role());
            assertEquals(Optional.of(Ids.id("n1")), node.raft.leaderId());
            assertEquals(List.of(new LogEntry.NoOp(1, 1)), output.persisted());
            var append = output.singleTo("n2", AppendEntries.class);
            assertEquals(new AppendEntries(1, 0, 0, List.of(new LogEntry.NoOp(1, 1)), 0), append);
        }

        @Test
        void candidateStepsDownAfterQuorumOfRejections() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.campaign();
            node.receive("n2", new PreVoteResponse(1, true));

            node.receive("n2", new RequestVoteResponse(1, false));
            node.receive("n3", new RequestVoteResponse(1, false));

            assertEquals(Role.FOLLOWER, node.raft.role());
            assertEquals(1, node.raft.term());
        }

        @Test
        void singleVoterBecomesLeaderAfterTimeout() {
            var node = TestNode.create("n1", "n1");

            var output = node.campaign();

            assertEquals(Role.LEADER, node.raft.role());
            assertEquals(1, node.raft.term());
            assertEquals(List.of(new LogEntry.NoOp(1, 1)), output.committed());
        }

        @Test
        void candidateRetriesWithPreVoteAfterTimeout() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.campaign();
            node.receive("n2", new PreVoteResponse(1, true));

            var output = node.tick(2 * TestNode.OPTIONS.electionTicks());

            assertEquals(Role.PRE_CANDIDATE, node.raft.role());
            assertEquals(1, node.raft.term());
            assertEquals(2, output.sentTo("n2", PreVote.class).getLast().term());
        }

        @Test
        void electionTimeoutIsRandomized() {
            var log = new InMemoryRaftLog();
            var raft = Raft.recover(Ids.id("n1"), Ids.voters("n1", "n2", "n3"), TestNode.OPTIONS, log,
                HardState.INITIAL, 0, TestNode.jitter(7));

            for (int i = 0; i < TestNode.OPTIONS.electionTicks() + 6; i++) {
                raft.step(new Input.Tick());
            }
            assertEquals(Role.FOLLOWER, raft.role());
            raft.step(new Input.Tick());
            assertEquals(Role.PRE_CANDIDATE, raft.role());
        }

        @Test
        void leaderStepsDownOnHigherTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");

            node.receive("n2", new Heartbeat(5, 0, 0));

            assertEquals(Role.FOLLOWER, node.raft.role());
            assertEquals(5, node.raft.term());
            assertEquals(Optional.of(Ids.id("n2")), node.raft.leaderId());
        }

        @Test
        void leaderStepsDownWithoutQuorumContact() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");

            node.tick(TestNode.OPTIONS.electionTicks());

            assertEquals(Role.FOLLOWER, node.raft.role());
        }

        @Test
        void leaderStaysLeaderWhileQuorumResponds() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");

            for (int i = 0; i < 3 * TestNode.OPTIONS.electionTicks(); i++) {
                node.tick(1);
                node.receive("n2", new HeartbeatResponse(node.raft.term(), 0));
            }

            assertEquals(Role.LEADER, node.raft.role());
        }

        @Test
        void staleLeaderMessagesAreAnsweredWithCurrentTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.receive("n2", new Heartbeat(3, 0, 0));

            var output = node.receive("n3", new Heartbeat(2, 0, 0));

            assertEquals(new HeartbeatResponse(3, 0), output.single(HeartbeatResponse.class));
            assertEquals(Optional.of(Ids.id("n2")), node.raft.leaderId());
        }

        @Test
        void learnerNeverCampaigns() {
            var node = TestNode.create("n3", Ids.membership(Set.of("n1", "n2"), Set.of("n3")));

            var output = node.tick(5 * TestNode.OPTIONS.electionTicks());

            assertEquals(Role.FOLLOWER, node.raft.role());
            assertTrue(output.messages().isEmpty());
        }
    }
}
