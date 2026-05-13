package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ClusterMembershipTest extends ClusterTestSupport {

    @Nested
    class MembershipChanges {
        @Test
        void addLearnerReplicatesAfterCommit() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            network.proposeConfigChange(leaderId, new MembershipChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.membership().isLearner("node-3"));

            var configurationChanges = network.drainMembershipChanges();
            long changesWithNewLearner = configurationChanges.stream()
                .filter(c -> c.current().isLearner("node-3"))
                .count();
            assertTrue(changesWithNewLearner >= 1);
        }

        @Test
        void promotedVoterAffectsQuorum() {
            var network = ClusterHarness.create(2, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            network.proposeConfigChange(leaderId, new MembershipChange.PromoteToVoter(ClusterHarness.nodeId(2)));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.membership().isVoter(ClusterHarness.nodeId(2)));

            String otherVoter = leaderId.equals(ClusterHarness.nodeId(0)) ? ClusterHarness.nodeId(1) : ClusterHarness.nodeId(0);
            network.isolate(otherVoter);

            long commitBefore = leader.commitIndex();
            network.propose(leaderId, "after-promotion".getBytes());

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.commitIndex() > commitBefore);
        }

        @Test
        void leaderStepsDownAfterSelfRemoval() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            network.proposeConfigChange(leaderId, new MembershipChange.RemoveNode(leaderId));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(RaftRole.FOLLOWER, leader.role());
        }

        @Test
        void configurationChangeCommitsWithMajority() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            network.isolate(ClusterHarness.nodeId(2));

            long commitBefore = leader.commitIndex();
            network.proposeConfigChange(leaderId, new MembershipChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.commitIndex() > commitBefore);
            assertTrue(leader.membership().isLearner("node-3"));
        }

        @Test
        void removedNodeStopsReceivingAppends() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var removedNodeId = ClusterHarness.nodeId(2);
            var removedNode = network.node(removedNodeId);

            network.proposeConfigChange(leaderId, new MembershipChange.RemoveNode(removedNodeId));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var leader = network.node(leaderId);
            assertFalse(leader.membership().isMember(removedNodeId));

            long commitBeforeProposal = removedNode.commitIndex();
            network.propose(leaderId, "after-removal".getBytes());

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(commitBeforeProposal, removedNode.commitIndex());
        }

        @Test
        void demotedVoterStillReplicates() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var demotedNodeId = ClusterHarness.nodeId(2);

            network.proposeConfigChange(leaderId, new MembershipChange.DemoteToLearner(demotedNodeId));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var leader = network.node(leaderId);
            var demotedNode = network.node(demotedNodeId);
            assertTrue(leader.membership().isLearner(demotedNodeId));

            network.propose(leaderId, "after-demotion".getBytes());

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(leader.commitIndex(), demotedNode.commitIndex());
        }

        @Test
        void newLearnerCatchesUpAfterJoining() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            for (int i = 0; i < 5; i++) {
                network.propose(leaderId, ("entry-" + i).getBytes());
            }

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            long commitBeforeJoin = leader.commitIndex();

            network.proposeConfigChange(leaderId, new MembershipChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var storage = new InMemoryRaftLog(leader.membership());
            var newLearner = RaftNode.builder("node-3", leader.membership(), RaftOptions.defaults(), storage)
                .electionJitter(_ -> 5)
                .build();
            network.addNode("node-3", newLearner, storage);

            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(leader.commitIndex(), newLearner.commitIndex());
        }

        @Test
        void configurationChangeEmitsTransition() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();

            network.proposeConfigChange(leaderId, new MembershipChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var changes = network.drainMembershipChanges();
            assertFalse(changes.isEmpty());

            var leaderChange = changes.stream()
                .filter(c -> c.nodeId().equals(leaderId))
                .findFirst()
                .orElseThrow();
            assertFalse(leaderChange.previous().isMember("node-3"));
            assertTrue(leaderChange.current().isLearner("node-3"));
        }
    }
}
