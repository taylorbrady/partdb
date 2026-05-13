package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeElectionTest extends RaftNodeTestSupport {

    @Nested
    class InitialState {
        @Test
        void startsAsFollower() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void startsAtTermZero() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.term());
        }

        @Test
        void startsWithNoLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertTrue(raft.leaderId().isEmpty());
        }

        @Test
        void startsWithCommitIndexZero() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            assertEquals(0, raft.commitIndex());
        }
    }

    @Nested
    class ElectionTimeout {
        @Test
        void followerStartsElectionAfterTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftInput.Tick());
            }
            assertEquals(RaftRole.FOLLOWER, raft.role());

            raft.step(new RaftInput.Tick());
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void candidateRestartsElectionAfterTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(1, raft.term());

            tickUntilTimeout(raft);
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(2, raft.term());
        }

        @Test
        void leaderDoesNotTimeoutElection() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());
            long term = raft.term();

            for (int i = 0; i < 50; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertTrue(raft.isLeader());
            assertEquals(term, raft.term());
        }

        @Test
        void electionTimeoutIsRandomized() {
            var counter = new AtomicInteger(0);
            var configuration = RaftMembership.voters("n1", "n2");
            var storage = new InMemoryRaftLog(configuration);
            var raft = RaftNode.builder("n1", configuration, RaftOptions.defaults(), storage)
                .electionJitter(_ -> counter.incrementAndGet())
                .build();

            for (int i = 0; i < 50; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertTrue(counter.get() > 1);
        }
    }

    @Nested
    class Voting {
        @Test
        void grantsVoteToFirstCandidateWithUpToDateLog() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertTrue(response.voteGranted());
        }

        @Test
        void rejectsVoteIfAlreadyVotedForAnother() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request1 = new RaftMessage.RequestVote(1, "n2", 0, 0);
            raft.step(new RaftInput.MessageReceived("n2", request1));

            var request2 = new RaftMessage.RequestVote(1, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request2));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void rejectsVoteIfCandidateLogBehind() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            var request = new RaftMessage.RequestVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void rejectsVoteFromLowerTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            var request = new RaftMessage.RequestVote(3, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", request));

            var response = findResponse(ready, RaftMessage.RequestVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void votingPersistsHardState() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var request = new RaftMessage.RequestVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", request));

            assertNotNull(ready.persistence().hardState().orElse(null));
            assertEquals(1, ready.persistence().hardState().orElseThrow().term());
            assertEquals("n2", ready.persistence().hardState().orElseThrow().votedFor());
        }
    }

    @Nested
    class LeaderElection {
        @Test
        void candidateBecomesLeaderWithMajority() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n2",
                new RaftMessage.RequestVoteResponse(1, true)));

            assertTrue(raft.isLeader());
        }

        @Test
        void candidateIgnoresPositiveVoteFromNonMember() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n4", new RaftMessage.RequestVoteResponse(1, true)));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertTrue(raft.isLeader());
        }

        @Test
        void singleNodeBecomesLeaderImmediately() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);

            assertTrue(raft.isLeader());
            assertEquals(1, raft.term());
        }

        @Test
        void leaderAppendsNoOpOnElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = becomeLeader(raft, List.of("n2", "n3"));

            assertFalse(ready.persistence().entries().isEmpty());
            var noOp = ready.persistence().entries().stream()
                .filter(e -> e instanceof RaftLogEntry.NoOp)
                .findFirst();
            assertTrue(noOp.isPresent());
        }

        @Test
        void leaderSendsAppendEntriesToPeersOnElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = becomeLeader(raft, List.of("n2", "n3"));

            long appendCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .count();
            assertEquals(2, appendCount);
        }
    }

    @Nested
    class TermChanges {
        @Test
        void stepDownToFollowerOnHigherTermMessage() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));
            assertTrue(raft.isLeader());

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals(5, raft.term());
        }

        @Test
        void rejectMessagesFromLowerTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            var lowerTermAppend = new RaftMessage.AppendEntries(3, "n3", 0, 0, List.of(), 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", lowerTermAppend));

            var response = findResponse(ready, RaftMessage.AppendEntriesResponse.class).orElseThrow();
            assertFalse(response.success());
            assertEquals(5, response.term());
        }

        @Test
        void updateTermOnHigherTermResponse() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));
            assertEquals(1, raft.term());

            var higherTermResponse = new RaftMessage.AppendEntriesResponse(5, false, 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermResponse));

            assertEquals(5, raft.term());
            assertEquals(RaftRole.FOLLOWER, raft.role());
        }
    }

    @Nested
    class VoteSplitting {
        @Test
        void electionRestartsWhenNoMajority() {
            var raft = createRaft("n1", "n1", "n2", "n3", "n4", "n5");

            tickUntilCandidate(raft, List.of("n2", "n3", "n4", "n5"));
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(1, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            tickUntilTimeout(raft);
            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(2, raft.term());
        }

        @Test
        void candidateVotesForItself() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, true)));
            assertTrue(raft.isLeader());
        }

        @Test
        void splitVoteLeadsToNewElection() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilCandidate(raft, List.of("n2", "n3"));
            assertEquals(1, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.RequestVoteResponse(1, false)));
            raft.step(new RaftInput.MessageReceived("n3", new RaftMessage.RequestVoteResponse(1, false)));
            assertEquals(RaftRole.CANDIDATE, raft.role());

            tickUntilTimeout(raft);
            assertEquals(2, raft.term());
            assertEquals(RaftRole.CANDIDATE, raft.role());
        }
    }

    @Nested
    class PreVote {
        @Test
        void followerStartsPreVoteOnTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            for (int i = 0; i < 9; i++) {
                raft.step(new RaftInput.Tick());
            }
            assertEquals(RaftRole.FOLLOWER, raft.role());

            raft.step(new RaftInput.Tick());
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
        }

        @Test
        void preVoteDoesNotIncrementTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);

            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void preVoteDoesNotPersistHardState() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.Tick());
            for (int i = 1; i < 10; i++) {
                ready = raft.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertNull(ready.persistence().hardState().orElse(null));
        }

        @Test
        void preCandidateSendsPreVoteRequests() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = tickUntilTimeout(raft);

            long preVoteCount = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.PreVote)
                .count();
            assertEquals(2, preVoteCount);

            var preVote = (RaftMessage.PreVote) ready.messages().get(0).message();
            assertEquals(1, preVote.term());
            assertEquals("n1", preVote.candidateId());
        }

        @Test
        void preCandidateBecomesFollowerOnAppendEntries() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals("n2", raft.leaderId().orElseThrow());
        }

        @Test
        void preCandidateBecomesFollowerOnHigherTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            var higherTermMsg = new RaftMessage.AppendEntries(5, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", higherTermMsg));

            assertEquals(RaftRole.FOLLOWER, raft.role());
            assertEquals(5, raft.term());
        }

        @Test
        void preCandidateBecomesCandidateWithMajorityPreVotes() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.PreVoteResponse(0, true)));

            assertEquals(RaftRole.CANDIDATE, raft.role());
            assertEquals(1, raft.term());
        }

        @Test
        void preVoteGrantedWhenLogUpToDateAndNoLeader() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);

            var preVote = new RaftMessage.PreVote(1, "n2", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n2", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertTrue(response.voteGranted());
        }

        @Test
        void preVoteDeniedWhenLeaderActive() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var heartbeat = new RaftMessage.AppendEntries(1, "n2", 0, 0, List.of(), 0);
            raft.step(new RaftInput.MessageReceived("n2", heartbeat));

            var preVote = new RaftMessage.PreVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void preVoteDeniedWhenLogBehind() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var appendEntries = new RaftMessage.AppendEntries(
                1, "n2", 0, 0,
                List.of(new RaftLogEntry.Data(1, 1, bytes("data"))),
                0
            );
            raft.step(new RaftInput.MessageReceived("n2", appendEntries));

            tickUntilTimeout(raft);

            var preVote = new RaftMessage.PreVote(2, "n3", 0, 0);
            var ready = raft.step(new RaftInput.MessageReceived("n3", preVote));

            var response = findResponse(ready, RaftMessage.PreVoteResponse.class).orElseThrow();
            assertFalse(response.voteGranted());
        }

        @Test
        void singleNodeSkipsPreVote() {
            var raft = createRaft("n1", "n1");

            tickUntilTimeout(raft);

            assertTrue(raft.isLeader());
            assertEquals(1, raft.term());
        }

        @Test
        void preCandidateRestartsPreVoteOnTimeout() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());
        }

        @Test
        void preVoteDoesNotUpdateTermOnHigherTermPreVote() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            assertEquals(0, raft.term());

            var preVote = new RaftMessage.PreVote(10, "n2", 0, 0);
            raft.step(new RaftInput.MessageReceived("n2", preVote));

            assertEquals(0, raft.term());
            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void preVoteResponseDoesNotUpdateTerm() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            tickUntilTimeout(raft);
            assertEquals(RaftRole.PRE_CANDIDATE, raft.role());
            assertEquals(0, raft.term());

            var response = new RaftMessage.PreVoteResponse(10, false);
            raft.step(new RaftInput.MessageReceived("n2", response));

            assertEquals(0, raft.term());
        }
    }
}
