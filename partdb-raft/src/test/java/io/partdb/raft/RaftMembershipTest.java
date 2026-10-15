package io.partdb.raft;

import io.partdb.raft.Message.AppendAccepted;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.PreVote;
import io.partdb.raft.Message.TimeoutNow;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftMembershipTest {

    private static Input.ChangeMembership change(long id, MembershipChange change) {
        return new Input.ChangeMembership(id, change);
    }

    private static TestNode leader() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        return node;
    }

    @Nested
    class Changes {
        @Test
        void usesNewMembershipAsSoonAsItIsAppended() {
            var node = leader();

            var proposed = node.step(change(1, new MembershipChange.AddLearner(Ids.id("n4"))));

            var expected = Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4"));
            assertEquals(List.of(new Outcome.Appended(1, 2, 1)), proposed.outcomes());
            assertEquals(List.of(new LogEntry.Configuration(2, 1, expected)), proposed.persisted());
            assertEquals(expected, node.raft.membership());
        }

        @Test
        void newMemberJoinsReplication() {
            var node = leader();
            node.step(change(1, new MembershipChange.AddLearner(Ids.id("n4"))));

            var output = node.receive("n2", new AppendAccepted(1, 2));

            assertEquals(1, output.sentTo("n4", AppendEntries.class).size());
        }

        @Test
        void revertsMembershipWhenItsEntryIsTruncated() {
            var node = TestNode.create("n2", "n1", "n2", "n3");
            var added = Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4"));
            node.receive("n1", new AppendEntries(1, 0, 0, List.of(new LogEntry.NoOp(1, 1), new LogEntry.Configuration(2, 1, added)), 1));
            assertEquals(added, node.raft.membership());

            node.receive("n3", new AppendEntries(2, 1, 1, List.of(new LogEntry.NoOp(2, 2)), 1));

            assertEquals(Ids.voters("n1", "n2", "n3"), node.raft.membership());
        }

        @Test
        void rejectsChangeWhileAnotherIsPending() {
            var node = leader();
            node.step(change(1, new MembershipChange.AddLearner(Ids.id("n4"))));

            var output = node.step(change(2, new MembershipChange.AddLearner(Ids.id("n5"))));

            assertEquals(List.of(new Outcome.Rejected(2, new Rejection.ChangeInProgress())), output.outcomes());
        }

        @Test
        void allowsNextChangeAfterPreviousIsApplied() {
            var node = leader();
            node.step(change(1, new MembershipChange.AddLearner(Ids.id("n4"))));
            node.receive("n2", new AppendAccepted(1, 2));

            var output = node.step(change(2, new MembershipChange.PromoteToVoter(Ids.id("n4"))));

            assertEquals(List.of(new Outcome.Appended(2, 3, 1)), output.outcomes());
        }

        @Test
        void rejectsChangeBeforeLeaderCommitsInItsTerm() {
            var node = TestNode.create("n1", "n1", "n2", "n3");
            node.elect("n2", "n3");

            var output = node.step(change(1, new MembershipChange.AddLearner(Ids.id("n4"))));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.ChangeInProgress())), output.outcomes());
        }

        @Test
        void rejectsChangeOnFollower() {
            var node = TestNode.create("n1", "n1", "n2", "n3");

            var output = node.step(change(1, new MembershipChange.AddLearner(Ids.id("n4"))));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.empty()))), output.outcomes());
        }

        @Test
        void rejectsInvalidChange() {
            var node = leader();

            var output = node.step(change(1, new MembershipChange.PromoteToVoter(Ids.id("n2"))));

            assertEquals(List.of(new Outcome.Rejected(1, new Rejection.Invalid("n2 must be a learner to be promoted"))), output.outcomes());
        }

        @Test
        void singleVoterAppliesChangeAfterPersisting() {
            var node = TestNode.create("n1", "n1");
            node.campaign();

            node.step(change(1, new MembershipChange.AddLearner(Ids.id("n2"))));

            assertTrue(node.raft.membership().isLearner(Ids.id("n2")));
        }

        @Test
        void removedMemberLeavesReplication() {
            var node = leader();
            node.step(change(1, new MembershipChange.RemoveNode(Ids.id("n3"))));
            node.receive("n2", new AppendAccepted(1, 2));

            var output = node.propose(1, "a");

            assertTrue(output.sentTo("n3", AppendEntries.class).isEmpty());
        }

        @Test
        void quorumFollowsNewMembership() {
            var node = leader();
            node.step(change(1, new MembershipChange.RemoveNode(Ids.id("n3"))));
            node.receive("n2", new AppendAccepted(1, 2));

            node.propose(1, "a");
            assertEquals(2, node.raft.commitIndex());
            node.receive("n2", new AppendAccepted(1, 3));

            assertEquals(3, node.raft.commitIndex());
        }

        @Test
        void leaderKeepsLeadingUntilItsRemovalCommitsThenHandsOff() {
            var node = leader();

            var proposed = node.step(change(1, new MembershipChange.RemoveNode(Ids.id("n1"))));

            assertEquals(List.of(new Outcome.Appended(1, 2, 1)), proposed.outcomes());
            assertEquals(Ids.voters("n2", "n3"), node.raft.membership());
            assertEquals(Role.LEADER, node.raft.role());

            node.receive("n2", new AppendAccepted(1, 2));
            assertEquals(Role.LEADER, node.raft.role());

            var committed = node.receive("n3", new AppendAccepted(1, 2));

            assertEquals(2, node.raft.commitIndex());
            assertEquals(Role.FOLLOWER, node.raft.role());
            assertEquals(1, committed.sent(TimeoutNow.class).size());
            assertTrue(node.tick(5 * TestNode.OPTIONS.electionTicks()).sent(PreVote.class).isEmpty());
        }

        @Test
        void demotedLeaderHandsOffAndStaysALearner() {
            var node = leader();
            node.step(change(1, new MembershipChange.DemoteToLearner(Ids.id("n1"))));

            node.receive("n2", new AppendAccepted(1, 2));
            node.receive("n3", new AppendAccepted(1, 2));

            assertEquals(Role.FOLLOWER, node.raft.role());
            assertTrue(node.raft.membership().isLearner(Ids.id("n1")));
        }

        @Test
        void removedNodeStopsCampaigning() {
            var node = TestNode.create("n3", "n1", "n2", "n3");
            node.receive("n1", new AppendEntries(1, 0, 0,
                List.of(new LogEntry.Configuration(1, 1, Ids.voters("n1", "n2"))), 1));

            var output = node.tick(5 * TestNode.OPTIONS.electionTicks());

            assertTrue(output.sent(PreVote.class).isEmpty());
        }

        @Test
        void followerUsesMembershipOnAppend() {
            var node = TestNode.create("n2", "n1", "n2", "n3");
            var next = Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4"));

            node.receive("n1", new AppendEntries(1, 0, 0, List.of(new LogEntry.Configuration(1, 1, next)), 0));

            assertEquals(next, node.raft.membership());
        }
    }

    @Nested
    class Learners {
        @Test
        void learnerReplicatesButDoesNotCountTowardsQuorum() {
            var node = TestNode.create("n1", Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4")));
            node.electAndCommit("n2", "n3");
            node.receive("n4", new AppendAccepted(1, 1));
            var output = node.propose(1, "a");

            assertEquals(1, output.sentTo("n4", AppendEntries.class).size());
            node.receive("n4", new AppendAccepted(1, 2));
            assertEquals(1, node.raft.commitIndex());
        }

        @Test
        void learnerIsNotAskedForVotes() {
            var node = TestNode.create("n1", Ids.membership(Set.of("n1", "n2", "n3"), Set.of("n4")));

            var output = node.campaign();

            assertTrue(output.sentTo("n4", PreVote.class).isEmpty());
            assertEquals(2, output.sent(PreVote.class).size());
        }
    }

    @Nested
    class MembershipRules {
        @Test
        void validatesTransitions() {
            var membership = Ids.voters("n1", "n2");

            assertThrows(IllegalArgumentException.class, () -> membership.apply(new MembershipChange.AddLearner(Ids.id("n1"))));
            assertThrows(IllegalArgumentException.class, () -> membership.apply(new MembershipChange.PromoteToVoter(Ids.id("n1"))));
            assertThrows(IllegalArgumentException.class, () -> membership.apply(new MembershipChange.RemoveNode(Ids.id("n9"))));
            assertThrows(IllegalArgumentException.class,
                () -> Ids.voters("n1").apply(new MembershipChange.RemoveNode(Ids.id("n1"))));
            assertThrows(IllegalArgumentException.class,
                () -> Ids.voters("n1").apply(new MembershipChange.DemoteToLearner(Ids.id("n1"))));
        }

        @Test
        void keepsMembersSorted() {
            var membership = Ids.membership(Set.of("c", "a", "b"), Set.of("e", "d"));

            assertEquals(List.of(Ids.id("a"), Ids.id("b"), Ids.id("c")), List.copyOf(membership.voters()));
            assertEquals(List.of(Ids.id("a"), Ids.id("b"), Ids.id("c"), Ids.id("d"), Ids.id("e")), List.copyOf(membership.members()));
            assertEquals(2, membership.quorum());
        }
    }
}
