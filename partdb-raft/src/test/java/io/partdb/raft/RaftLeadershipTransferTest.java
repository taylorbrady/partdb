package io.partdb.raft;

import io.partdb.raft.RaftEffects.ProposalResult;
import io.partdb.raft.RaftMessage.AppendAccepted;
import io.partdb.raft.RaftMessage.Heartbeat;
import io.partdb.raft.RaftMessage.RequestVote;
import io.partdb.raft.RaftMessage.TimeoutNow;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftLeadershipTransferTest {

    private static RaftInput.TransferLeadership transferTo(String target) {
        return new RaftInput.TransferLeadership(target);
    }

    @Test
    void sendsTimeoutNowToCaughtUpTarget() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");

        var output = node.step(transferTo("n2"));

        assertEquals(new TimeoutNow(1), output.singleTo("n2", TimeoutNow.class));
    }

    @Test
    void catchesTargetUpBeforeSendingTimeoutNow() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");

        assertTrue(node.step(transferTo("n2")).sent(TimeoutNow.class).isEmpty());

        var output = node.receive("n2", new AppendAccepted(1, 2));
        assertEquals(new TimeoutNow(1), output.singleTo("n2", TimeoutNow.class));
    }

    @Test
    void rejectsProposalsDuringTransfer() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");
        node.step(transferTo("n2"));

        var output = node.propose(2, "b");

        assertEquals(List.of(new ProposalResult.Rejected(2)), output.proposals());
    }

    @Test
    void abortsTransferAfterElectionTimeout() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");
        node.step(transferTo("n2"));

        for (int i = 0; i < TestNode.OPTIONS.electionTicks(); i++) {
            node.tick(1);
            node.receive("n3", new RaftMessage.HeartbeatResponse(1, 0));
        }

        assertEquals(RaftRole.LEADER, node.raft.role());
        assertEquals(List.of(new ProposalResult.Accepted(2, 3, 1)), node.propose(2, "b").proposals());
    }

    @Test
    void ignoresTransferToNonVoterOrSelf() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");

        assertTrue(node.step(transferTo("n1"), transferTo("n9")).sent(TimeoutNow.class).isEmpty());
        assertEquals(List.of(new ProposalResult.Accepted(1, 2, 1)), node.propose(1, "a").proposals());
    }

    @Test
    void targetCampaignsImmediatelyWithoutPreVote() {
        var node = TestNode.create("n2", "n1", "n2", "n3");
        node.receive("n1", new Heartbeat(1, 0, 0));

        var output = node.receive("n1", new TimeoutNow(1));

        assertEquals(RaftRole.CANDIDATE, node.raft.role());
        assertEquals(new RequestVote(2, 0, 0, true), output.singleTo("n3", RequestVote.class));
    }

    @Test
    void ignoresTimeoutNowFromNonLeader() {
        var node = TestNode.create("n2", "n1", "n2", "n3");
        node.receive("n1", new Heartbeat(1, 0, 0));

        node.receive("n3", new TimeoutNow(1));

        assertEquals(RaftRole.FOLLOWER, node.raft.role());
    }
}
