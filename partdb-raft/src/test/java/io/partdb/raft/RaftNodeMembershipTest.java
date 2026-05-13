package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeMembershipTest extends RaftNodeTestSupport {

    @Nested
    class MembershipChanges {
        @Test
        void leaderAddsLearner() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n4"));
        }

        @Test
        void leaderPromotesLearner() {
            var configuration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
            var raft = createRaft("n1", configuration, CONFIG);
            becomeLeader(raft, List.of("n2"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.PromoteToVoter("n3")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isVoter("n3"));
            assertFalse(configEntry.get().membership().isLearner("n3"));
        }

        @Test
        void leaderDemotesVoter() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n3")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n3"));
            assertFalse(configEntry.get().membership().isVoter("n3"));
        }

        @Test
        void leaderRemovesNode() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n3")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertFalse(configEntry.get().membership().isMember("n3"));
        }

        @Test
        void nonLeaderIgnoresConfigChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));

            assertTrue(ready.persistence().entries().isEmpty());
            assertTrue(ready.messages().isEmpty());
        }

        @Test
        void rejectSecondPendingConfigChange() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n5")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void singleNodeLeaderCommitsConfigImmediately() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n2")));

            assertEquals(1, ready.application().membershipTransitions().size());
            var change = ready.application().membershipTransitions().getFirst();
            assertFalse(change.previous().isMember("n2"));
            assertTrue(change.current().isLearner("n2"));
        }

        @Test
        void configurationChangeUpdatesQuorum() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n3")));

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(2, raft.commitIndex());
        }

        @Test
        void leaderStepsDownAfterSelfRemoval() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n1")));

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void removedNodeDoesNotCampaignAfterSelfRemoval() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n1")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            for (int i = 0; i < CONFIG.electionTimeoutMax() * 2; i++) {
                raft.step(new RaftInput.Tick());
            }

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void leaderStepsDownAfterSelfDemotion() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n1")));

            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertEquals(RaftRole.FOLLOWER, raft.role());
        }

        @Test
        void configurationChangeAppliesOnCommit() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));
            assertFalse(raft.membership().isLearner("n4"));

            var ready = raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            assertTrue(raft.membership().isLearner("n4"));
            assertEquals(1, ready.application().membershipTransitions().size());
        }

        @Test
        void cannotDemoteLastVoter() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.DemoteToLearner("n1")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotRemoveLastVoter() {
            var raft = createRaft("n1", "n1");
            tickUntilTimeout(raft);
            assertTrue(raft.isLeader());

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n1")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotAddExistingMember() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n2")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void cannotPromoteNonLearner() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.PromoteToVoter("n4")));

            assertTrue(findEntry(ready, RaftLogEntry.Config.class).isEmpty());
        }

        @Test
        void allowsSecondConfigChangeAfterFirstCommits() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n5")));

            var configEntry = findEntry(ready, RaftLogEntry.Config.class);
            assertTrue(configEntry.isPresent());
            assertTrue(configEntry.get().membership().isLearner("n5"));
        }

        @Test
        void newPeerAddedToReplication() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.AddLearner("n4")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = tickHeartbeat(raft);

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(RaftEffects.Outbound::to)
                .toList();
            assertTrue(targets.contains("n4"));
        }

        @Test
        void removedPeerExcludedFromReplication() {
            var raft = createRaft("n1", "n1", "n2", "n3");
            becomeLeader(raft, List.of("n2", "n3"));

            raft.step(new RaftInput.MembershipChangeProposed(new MembershipChange.RemoveNode("n3")));
            raft.step(new RaftInput.MessageReceived("n2", new RaftMessage.AppendEntriesResponse(1, true, 2)));

            var ready = tickHeartbeat(raft);

            var targets = ready.messages().stream()
                .filter(m -> m.message() instanceof RaftMessage.AppendEntries)
                .map(RaftEffects.Outbound::to)
                .toList();
            assertFalse(targets.contains("n3"));
        }
    }
}
