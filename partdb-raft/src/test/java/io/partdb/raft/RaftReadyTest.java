package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.AppendAccepted;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.RequestVote;
import io.partdb.raft.Message.RequestVoteResponse;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.partdb.raft.Ids.id;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftReadyTest {

    @Test
    void leaderReplicatesEntriesBeforeTheyAreDurableLocally() {
        var leader = TestNode.create("n1", "n1", "n2", "n3");
        leader.electAndCommit("n2", "n3");

        var ready = step(leader, new Input.Propose(1, Bytes.utf8("a")));

        assertEquals(1, ready.entries().size());
        assertEquals(Set.of(id("n2"), id("n3")), Set.copyOf(ready.messages().stream().map(Envelope::to).toList()));
        ready.messages().forEach(envelope -> assertEquals(ready.entries(), assertInstanceOf(AppendEntries.class, envelope.message()).entries()));
        assertTrue(ready.persistedMessages().isEmpty());
    }

    @Test
    void leaderCommitsWithoutItsOwnCopyOnlyWhenTheOtherVotersFormAMajority() {
        var leader = TestNode.create("n1", "n1", "n2", "n3");
        leader.electAndCommit("n2", "n3");
        long index = step(leader, new Input.Propose(1, Bytes.utf8("a"))).entries().getFirst().index();
        long committed = leader.raft.commitIndex();

        step(leader, new Input.Receive(id("n2"), new AppendAccepted(leader.raft.term(), index)));
        assertEquals(committed, leader.raft.commitIndex());

        var ready = step(leader, new Input.Receive(id("n3"), new AppendAccepted(leader.raft.term(), index)));
        assertEquals(index, leader.raft.commitIndex());
        assertEquals(index, ready.committed().getLast().index());
    }

    @Test
    void leaderHeartbeatsGoOutImmediately() {
        var leader = TestNode.create("n1", "n1", "n2", "n3");
        leader.electAndCommit("n2", "n3");

        var ready = step(leader, ticks(TestNode.OPTIONS.heartbeatTicks()));

        assertEquals(2, ready.messages().stream().filter(envelope -> envelope.message() instanceof Heartbeat).count());
        assertTrue(ready.persistedMessages().isEmpty());
    }

    @Test
    void followerAcknowledgesEntriesOnlyAfterPersistingThem() {
        var follower = TestNode.create("n2", "n1", "n2", "n3");

        var ready = step(follower, new Input.Receive(id("n1"),
            new AppendEntries(1, 0, 0, List.of(new LogEntry.Command(1, 1, Bytes.utf8("a"))), 0)));

        assertEquals(1, ready.entries().size());
        assertTrue(ready.messages().isEmpty());
        assertEquals(new AppendAccepted(1, 1), single(ready.persistedMessages()));
    }

    @Test
    void voteIsGrantedOnlyAfterItIsDurable() {
        var voter = TestNode.create("n2", "n1", "n2", "n3");

        var ready = step(voter, new Input.Receive(id("n1"), new RequestVote(1, 0, 0, false)));

        assertTrue(ready.messages().isEmpty());
        assertEquals(new RequestVoteResponse(1, true), single(ready.persistedMessages()));
    }

    @Test
    void candidateRequestsVotesOnlyAfterItsTermAndVoteAreDurable() {
        var candidate = TestNode.create("n1", "n1", "n2", "n3");
        candidate.campaign();

        var ready = step(candidate, new Input.Receive(id("n2"), new Message.PreVoteResponse(1, true)));

        assertEquals(Role.CANDIDATE, candidate.raft.role());
        assertTrue(ready.messages().isEmpty());
        assertTrue(ready.persistedMessages().stream().allMatch(envelope -> envelope.message() instanceof RequestVote));
    }

    @Test
    void appendsWaitWhenTheSameReadyChangesTheTerm() {
        var leader = TestNode.create("n1", Ids.membership(Set.of("n1"), Set.of("n2")));

        var ready = step(leader, ticks(TestNode.OPTIONS.electionTicks()));

        assertEquals(Role.LEADER, leader.raft.role());
        assertTrue(ready.hardState().isPresent());
        assertTrue(ready.messages().isEmpty());
        assertInstanceOf(AppendEntries.class, single(ready.persistedMessages()));
    }

    private static Ready step(TestNode node, Input... inputs) {
        for (Input input : inputs) {
            node.raft.step(input);
        }
        var ready = node.raft.ready();
        node.log.persist(ready);
        return ready;
    }

    private static Input[] ticks(int count) {
        var ticks = new Input[count];
        Arrays.fill(ticks, new Input.Tick());
        return ticks;
    }

    private static Message single(List<Envelope> envelopes) {
        assertEquals(1, envelopes.size(), envelopes::toString);
        return envelopes.getFirst().message();
    }
}
