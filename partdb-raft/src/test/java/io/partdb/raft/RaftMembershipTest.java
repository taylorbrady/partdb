package io.partdb.raft;

import io.partdb.raft.RaftEffects.ProposalResult;
import io.partdb.raft.RaftMessage.AppendAccepted;
import io.partdb.raft.RaftMessage.AppendEntries;
import io.partdb.raft.RaftMessage.PreVote;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftMembershipTest {

    private static RaftInput.ChangeMembership change(long id, MembershipChange change) {
        return new RaftInput.ChangeMembership(id, change);
    }

    private static TestNode leader() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        return node;
    }

    @Nested
    class Changes {
        @Test
        void appendsMembershipEntryAndAppliesItOnCommit() {
            var node = leader();

            var proposed = node.step(change(1, new MembershipChange.AddLearner("n4")));

            var expected = RaftMembership.of(Set.of("n1", "n2", "n3"), Set.of("n4"));
            assertEquals(List.of(new ProposalResult.Accepted(1, 2, 1)), proposed.proposals());
            assertEquals(List.of(new RaftLogEntry.Membership(2, 1, expected)), proposed.persisted());
            assertFalse(node.raft.membership().isMember("n4"));

            node.receive("n2", new AppendAccepted(1, 2));

            assertEquals(expected, node.raft.membership());
        }

        @Test
        void rejectsChangeWhileAnotherIsPending() {
            var node = leader();
            node.step(change(1, new MembershipChange.AddLearner("n4")));

            var output = node.step(change(2, new MembershipChange.AddLearner("n5")));

            assertEquals(List.of(new ProposalResult.Rejected(2)), output.proposals());
        }

        @Test
        void allowsNextChangeAfterPreviousIsApplied() {
            var node = leader();
            node.step(change(1, new MembershipChange.AddLearner("n4")));
            node.receive("n2", new AppendAccepted(1, 2));

            var output = node.step(change(2, new MembershipChange.PromoteToVoter("n4")));

            assertEquals(List.of(new ProposalResult.Accepted(2, 3, 1)), output.proposals());
        }

        @Test
        void rejectsChangeBeforeLeaderCommitsInItsTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");

            var output = node.step(change(1, new MembershipChange.AddLearner("n4")));

            assertEquals(List.of(new ProposalResult.Rejected(1)), output.proposals());
        }

        @Test
        void rejectsChangeOnFollower() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.step(change(1, new MembershipChange.AddLearner("n4")));

            assertEquals(List.of(new ProposalResult.Rejected(1)), output.proposals());
        }

        @Test
        void rejectsInvalidChange() {
            var node = leader();

            var output = node.step(change(1, new MembershipChange.PromoteToVoter("n2")));

            assertEquals(List.of(new ProposalResult.Rejected(1)), output.proposals());
        }

        @Test
        void singleVoterAppliesChangeAfterPersisting() {
            var node = TestNode.create("n1", "n1");
            node.campaign();

            node.step(change(1, new MembershipChange.AddLearner("n2")));

            assertTrue(node.raft.membership().isLearner("n2"));
        }

        @Test
        void newMemberJoinsReplication() {
            var node = leader();
            node.step(change(1, new MembershipChange.AddLearner("n4")));

            var output = node.receive("n2", new AppendAccepted(1, 2));

            assertEquals(1, output.sentTo("n4", AppendEntries.class).size());
        }

        @Test
        void removedMemberLeavesReplication() {
            var node = leader();
            node.step(change(1, new MembershipChange.RemoveNode("n3")));
            node.receive("n2", new AppendAccepted(1, 2));

            var output = node.propose(1, "a");

            assertTrue(output.sentTo("n3", AppendEntries.class).isEmpty());
        }

        @Test
        void quorumFollowsNewMembership() {
            var node = leader();
            node.step(change(1, new MembershipChange.RemoveNode("n3")));
            node.receive("n2", new AppendAccepted(1, 2));

            node.propose(1, "a");
            assertEquals(2, node.raft.commitIndex());
            node.receive("n2", new AppendAccepted(1, 3));

            assertEquals(3, node.raft.commitIndex());
        }

        @Test
        void leaderStepsDownWhenItIsRemoved() {
            var node = leader();
            node.step(change(1, new MembershipChange.RemoveNode("n1")));

            node.receive("n2", new AppendAccepted(1, 2));

            assertEquals(RaftRole.FOLLOWER, node.raft.role());
            assertFalse(node.raft.membership().isMember("n1"));
            assertTrue(node.tick(5 * TestNode.OPTIONS.electionTicks()).sent(PreVote.class).isEmpty());
        }

        @Test
        void leaderStepsDownWhenItIsDemoted() {
            var node = leader();
            node.step(change(1, new MembershipChange.DemoteToLearner("n1")));

            node.receive("n2", new AppendAccepted(1, 2));

            assertEquals(RaftRole.FOLLOWER, node.raft.role());
            assertTrue(node.raft.membership().isLearner("n1"));
        }

        @Test
        void followerAppliesMembershipOnCommit() {
            var node = TestNode.create("n2", "n1", "n2", "n3");
            var next = RaftMembership.of(Set.of("n1", "n2", "n3"), Set.of("n4"));
            node.receive("n1", new AppendEntries(1, 0, 0, List.of(new RaftLogEntry.Membership(1, 1, next)), 0));
            assertFalse(node.raft.membership().isMember("n4"));

            node.receive("n1", new AppendEntries(1, 1, 1, List.of(), 1));

            assertEquals(next, node.raft.membership());
        }
    }

    @Nested
    class Learners {
        @Test
        void learnerReplicatesButDoesNotCountTowardsQuorum() {
            var node = TestNode.create("n1", RaftMembership.of(Set.of("n1", "n2", "n3"), Set.of("n4")));
            node.electAndCommit("n2", "n3");
            node.receive("n4", new AppendAccepted(1, 1));
            var output = node.propose(1, "a");

            assertEquals(1, output.sentTo("n4", AppendEntries.class).size());
            node.receive("n4", new AppendAccepted(1, 2));
            assertEquals(1, node.raft.commitIndex());
        }

        @Test
        void learnerIsNotAskedForVotes() {
            var node = TestNode.create("n1", RaftMembership.of(Set.of("n1", "n2", "n3"), Set.of("n4")));

            var output = node.campaign();

            assertTrue(output.sentTo("n4", PreVote.class).isEmpty());
            assertEquals(2, output.sent(PreVote.class).size());
        }
    }

    @Nested
    class MembershipRules {
        @Test
        void validatesTransitions() {
            var membership = RaftMembership.voters("n1", "n2");

            assertThrows(IllegalArgumentException.class, () -> membership.apply(new MembershipChange.AddLearner("n1")));
            assertThrows(IllegalArgumentException.class, () -> membership.apply(new MembershipChange.PromoteToVoter("n1")));
            assertThrows(IllegalArgumentException.class, () -> membership.apply(new MembershipChange.RemoveNode("n9")));
            assertThrows(IllegalArgumentException.class,
                () -> RaftMembership.voters("n1").apply(new MembershipChange.RemoveNode("n1")));
            assertThrows(IllegalArgumentException.class,
                () -> RaftMembership.voters("n1").apply(new MembershipChange.DemoteToLearner("n1")));
        }

        @Test
        void keepsMembersSorted() {
            var membership = RaftMembership.of(Set.of("c", "a", "b"), Set.of("e", "d"));

            assertEquals(List.of("a", "b", "c"), List.copyOf(membership.voters()));
            assertEquals(List.of("a", "b", "c", "d", "e"), List.copyOf(membership.members()));
            assertEquals(2, membership.quorum());
        }
    }
}
