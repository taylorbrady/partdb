package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.AppendAccepted;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.RequestVote;
import io.partdb.raft.Message.TimeoutNow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftLeadershipTransferTest {

    private static Input.TransferLeadership transferTo(long id, String target) {
        return new Input.TransferLeadership(id, Ids.id(target));
    }

    @Test
    void sendsTimeoutNowToCaughtUpTarget() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");

        var output = node.step(transferTo(1, "n2"));

        assertEquals(new TimeoutNow(1), output.singleTo("n2", TimeoutNow.class));
        assertTrue(output.outcomes().isEmpty());
    }

    @Test
    void catchesTargetUpBeforeSendingTimeoutNow() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");

        assertTrue(node.step(transferTo(2, "n2")).sent(TimeoutNow.class).isEmpty());

        var output = node.receive("n2", new AppendAccepted(1, 2));
        assertEquals(new TimeoutNow(1), output.singleTo("n2", TimeoutNow.class));
    }

    @Test
    void completesWhenTheTargetLeads() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.step(transferTo(1, "n2"));

        node.receive("n2", new RequestVote(2, 1, 1, true));
        var output = node.receive("n2", new AppendEntries(2, 1, 1, List.of(), 1));

        assertEquals(List.of(new Outcome.Transferred(1)), output.outcomes());
    }

    @Test
    void failsWhenAnotherNodeLeads() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.step(transferTo(1, "n2"));

        var output = node.receive("n3", new AppendEntries(2, 1, 1, List.of(), 1));

        assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.of(Ids.id("n3"))))), output.outcomes());
    }

    @Test
    void rejectsProposalsAndSecondTransfersDuringTransfer() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");
        node.step(transferTo(2, "n2"));

        var output = node.step(new Input.Propose(3, Bytes.utf8("b")), transferTo(4, "n3"));

        assertEquals(List.of(
            new Outcome.Rejected(3, new Rejection.TransferInProgress()),
            new Outcome.Rejected(4, new Rejection.TransferInProgress())
        ), output.outcomes());
    }

    @Test
    void abortsTransferAfterElectionTimeout() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");
        node.step(transferTo(2, "n2"));

        var outcomes = new ArrayList<Outcome>();
        for (int i = 0; i < TestNode.OPTIONS.electionTicks(); i++) {
            outcomes.addAll(node.tick(1).outcomes());
            node.receive("n3", new Message.HeartbeatResponse(1, 0));
        }

        assertEquals(List.of(new Outcome.Rejected(2, new Rejection.Aborted())), outcomes);
        assertEquals(Role.LEADER, node.raft.role());
        assertEquals(List.of(new Outcome.Appended(3, 3, 1)), node.propose(3, "b").outcomes());
    }

    @Test
    void transferToSelfCompletesAndToNonVoterIsInvalid() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");

        var output = node.step(transferTo(1, "n1"), transferTo(2, "n9"));

        assertTrue(output.sent(TimeoutNow.class).isEmpty());
        assertEquals(List.of(
            new Outcome.Transferred(1),
            new Outcome.Rejected(2, new Rejection.Invalid("n9 is not a voter"))
        ), output.outcomes());
        assertEquals(List.of(new Outcome.Appended(3, 2, 1)), node.propose(3, "a").outcomes());
    }

    @Test
    void stepDownHandsLeadershipToTheMostCaughtUpVoter() {
        var node = TestNode.create("n1", "n1", "n2", "n3");
        node.electAndCommit("n2", "n3");
        node.propose(1, "a");
        node.receive("n3", new AppendAccepted(1, 2));

        var output = node.step(new Input.StepDown(2));

        assertEquals(new TimeoutNow(1), output.singleTo("n3", TimeoutNow.class));
        assertTrue(output.sentTo("n2", TimeoutNow.class).isEmpty());
    }

    @Test
    void stepDownIsRejectedWithoutAnotherVoter() {
        var node = TestNode.create("n1", "n1");
        node.campaign();

        var output = node.step(new Input.StepDown(1));

        assertEquals(List.of(new Outcome.Rejected(1, new Rejection.Invalid("no other voter can take over leadership"))), output.outcomes());
    }

    @Test
    void followerRejectsTransfer() {
        var node = TestNode.create("n1", "n1", "n2", "n3");

        var output = node.step(transferTo(1, "n2"));

        assertEquals(List.of(new Outcome.Rejected(1, new Rejection.NotLeader(Optional.empty()))), output.outcomes());
    }

    @Test
    void targetCampaignsImmediatelyWithoutPreVote() {
        var node = TestNode.create("n2", "n1", "n2", "n3");
        node.receive("n1", new Heartbeat(1, 0, 0));

        var output = node.receive("n1", new TimeoutNow(1));

        assertEquals(Role.CANDIDATE, node.raft.role());
        assertEquals(new RequestVote(2, 0, 0, true), output.singleTo("n3", RequestVote.class));
    }

    @Test
    void ignoresTimeoutNowFromNonLeader() {
        var node = TestNode.create("n2", "n1", "n2", "n3");
        node.receive("n1", new Heartbeat(1, 0, 0));

        node.receive("n3", new TimeoutNow(1));

        assertEquals(Role.FOLLOWER, node.raft.role());
    }
}
