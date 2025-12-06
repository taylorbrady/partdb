package io.partdb.raft;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ClusterTest {

    private void electLeader(Network network) {
        network.runUntil(n -> n.leader().isPresent(), 50);
    }

    @Nested
    class LeaderElection {
        @Test
        void singleNodeClusterElectsLeader() {
            var network = Network.create(1);

            electLeader(network);

            assertTrue(network.node(0).isLeader());
        }

        @Test
        void threeNodeClusterElectsLeader() {
            var network = Network.create(3);

            electLeader(network);

            assertEquals(1, network.leaderCount());
        }

        @Test
        void fiveNodeClusterElectsLeader() {
            var network = Network.create(5);

            electLeader(network);

            assertEquals(1, network.leaderCount());
        }

        @Test
        void atMostOneLeaderPerTerm() {
            var network = Network.create(5);

            for (int i = 0; i < 100; i++) {
                network.tick();
                network.deliverAll();

                Map<Long, Set<String>> leadersPerTerm = new HashMap<>();
                for (var node : network.allNodes()) {
                    if (node.isLeader()) {
                        leadersPerTerm
                            .computeIfAbsent(node.term(), _ -> new HashSet<>())
                            .add(node.leaderId().orElseThrow());
                    }
                }

                for (var leaders : leadersPerTerm.values()) {
                    assertTrue(leaders.size() <= 1,
                        "Multiple leaders in same term: " + leaders);
                }
            }
        }

        @Test
        void leaderKnowsItsOwnId() {
            var network = Network.create(3);

            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            assertEquals(leaderId, leader.leaderId().orElseThrow());
        }
    }

    @Nested
    class Partitions {
        @Test
        void majorityElectsNewLeaderDuringPartition() {
            var network = Network.create(3);
            electLeader(network);

            var oldLeaderId = network.leader().orElseThrow();
            network.isolate(oldLeaderId);

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            long connectedLeaders = network.allNodes().stream()
                .filter(n -> n.isLeader() && !n.leaderId().orElseThrow().equals(oldLeaderId))
                .count();
            assertTrue(connectedLeaders >= 1);
        }

        @Test
        void healedNodeRejoinsAndFollowsNewLeader() {
            var network = Network.create(3);
            electLeader(network);

            network.isolate(Network.nodeId(2));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(Network.nodeId(2));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(1, network.leaderCount());
        }

        @Test
        void oldLeaderBecomesFollowerOnRejoin() {
            var network = Network.create(3);
            electLeader(network);

            var oldLeaderId = network.leader().orElseThrow();
            var oldLeader = network.node(oldLeaderId);
            long oldTerm = oldLeader.term();

            network.isolate(oldLeaderId);

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(oldLeaderId);

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(Role.FOLLOWER, oldLeader.role());
            assertTrue(oldLeader.term() > oldTerm);
        }

        @Test
        void minorityCannotElectLeader() {
            var network = Network.create(5);
            electLeader(network);

            network.isolate(Network.nodeId(0));
            network.isolate(Network.nodeId(1));
            network.isolate(Network.nodeId(2));

            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            var node3 = network.node(3);
            var node4 = network.node(4);
            assertFalse(node3.isLeader() && node4.isLeader());
        }
    }

    @Nested
    class Replication {
        @Test
        void proposalReplicatesToAllNodes() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.propose(leaderId, "hello".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            for (var node : network.allNodes()) {
                assertTrue(node.commitIndex() >= 2);
            }
        }

        @Test
        void proposalCommitsWithMajority() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(Network.nodeId(2));

            network.propose(leaderId, "hello".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var leader = network.node(leaderId);
            assertTrue(leader.commitIndex() >= 2);
        }

        @Test
        void laggedFollowerCatchesUp() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(Network.nodeId(2));

            network.propose(leaderId, "entry1".getBytes());
            network.propose(leaderId, "entry2".getBytes());
            network.propose(leaderId, "entry3".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(Network.nodeId(2));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var lagged = network.node(2);
            var leader = network.node(leaderId);
            assertEquals(leader.commitIndex(), lagged.commitIndex());
        }

        @Test
        void multipleProposalsReplicateInOrder() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            for (int i = 0; i < 5; i++) {
                network.propose(leaderId, ("entry-" + i).getBytes());
            }

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            long minCommit = network.allNodes().stream()
                .mapToLong(Raft::commitIndex)
                .min()
                .orElse(0);

            assertTrue(minCommit >= 6);
        }
    }

    @Nested
    class Consistency {
        @Test
        void allNodesAgreeOnCommittedEntries() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            for (int i = 0; i < 10; i++) {
                network.propose(leaderId, ("data-" + i).getBytes());
                network.tick();
                network.deliverAll();
            }

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            long leaderCommit = network.node(leaderId).commitIndex();
            for (var node : network.allNodes()) {
                assertEquals(leaderCommit, node.commitIndex());
            }
        }

        @Test
        void termsOnlyIncrease() {
            var network = Network.create(3);

            Map<String, Long> lastSeenTerm = new HashMap<>();
            for (var node : network.allNodes()) {
                lastSeenTerm.put(node.leaderId().orElse("unknown"), 0L);
            }

            for (int i = 0; i < 100; i++) {
                network.tick();
                network.deliverAll();

                for (var node : network.allNodes()) {
                    String nodeId = node.leaderId().orElse("unknown");
                    long term = node.term();
                    assertTrue(term >= lastSeenTerm.getOrDefault(nodeId, 0L));
                }
            }
        }
    }

    @Nested
    class PreVoteCluster {
        @Test
        void clusterElectsLeaderWithPreVote() {
            var network = Network.create(3);

            electLeader(network);

            assertEquals(1, network.leaderCount());
            var leader = network.node(network.leader().orElseThrow());
            assertTrue(leader.term() >= 1);
        }

        @Test
        void partitionedNodeDoesNotInflateTerm() {
            var network = Network.create(3);
            electLeader(network);

            var isolatedNode = network.node(2);
            var termBeforeIsolation = isolatedNode.term();

            network.isolate(Network.nodeId(2));

            for (int i = 0; i < 100; i++) {
                network.tickNode(Network.nodeId(2));
            }

            var termDuringIsolation = isolatedNode.term();
            assertEquals(termBeforeIsolation, termDuringIsolation);
        }

        @Test
        void healedNodeRejoinsWithoutTermInflation() {
            var network = Network.create(3);
            electLeader(network);

            var isolatedNode = network.node(2);
            var termBeforeIsolation = isolatedNode.term();

            network.isolate(Network.nodeId(2));

            for (int i = 0; i < 100; i++) {
                network.tickNode(Network.nodeId(2));
            }

            assertEquals(termBeforeIsolation, isolatedNode.term());

            network.heal(Network.nodeId(2));

            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(1, network.leaderCount());
        }

        @Test
        void preVoteAllowsElectionWhenLeaderFails() {
            var network = Network.create(3);
            electLeader(network);

            var oldLeaderId = network.leader().orElseThrow();
            var oldTerm = network.node(oldLeaderId).term();

            network.isolate(oldLeaderId);

            for (int i = 0; i < 100; i++) {
                network.tick();
                network.deliverAll();
            }

            var remainingNodes = network.allNodes().stream()
                .filter(n -> !n.leaderId().equals(Optional.of(oldLeaderId)) || n.isLeader())
                .filter(Raft::isLeader)
                .count();

            assertTrue(remainingNodes >= 1);
        }
    }

    @Nested
    class SnapshotRecovery {
        @Test
        void laggedFollowerReceivesSnapshotWhenLogCompacted() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(Network.nodeId(2));

            for (int i = 0; i < 5; i++) {
                network.propose(leaderId, ("entry-" + i).getBytes());
            }

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var leader = network.node(leaderId);
            long commitBeforeCompaction = leader.commitIndex();
            assertTrue(commitBeforeCompaction >= 5);

            network.heal(Network.nodeId(2));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var lagged = network.node(2);
            assertEquals(leader.commitIndex(), lagged.commitIndex());
        }
    }

    @Nested
    class ReadIndex {
        @Test
        void leaderServesReadIndexDirectly() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            long commitBefore = leader.commitIndex();

            byte[] context = "read-1".getBytes();
            network.readIndex(leaderId, context);

            for (int i = 0; i < 10; i++) {
                network.tick();
                network.deliverAll();
            }

            var readStates = network.drainReadStates();
            assertEquals(1, readStates.size());

            var readState = readStates.getFirst();
            assertEquals(leaderId, readState.nodeId());
            assertTrue(readState.index() >= commitBefore);
            assertArrayEquals(context, readState.context());
        }

        @Test
        void followerForwardsReadIndexToLeader() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            String followerId = Network.nodeId(0).equals(leaderId) ? Network.nodeId(1) : Network.nodeId(0);

            byte[] context = "follower-read".getBytes();
            network.readIndex(followerId, context);

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var readStates = network.drainReadStates();
            assertEquals(1, readStates.size());

            var readState = readStates.getFirst();
            assertEquals(followerId, readState.nodeId());
            assertArrayEquals(context, readState.context());
        }

        @Test
        void readIndexWorksAfterLeaderChange() {
            var network = Network.create(3);
            electLeader(network);

            var oldLeaderId = network.leader().orElseThrow();

            network.isolate(oldLeaderId);
            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            var newLeader = network.allNodes().stream()
                .filter(n -> n.isLeader() && !n.leaderId().orElseThrow().equals(oldLeaderId))
                .findFirst();
            assertTrue(newLeader.isPresent());

            var newLeaderId = newLeader.get().leaderId().orElseThrow();

            byte[] context = "new-leader-read".getBytes();
            network.readIndex(newLeaderId, context);

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var readStates = network.drainReadStates();
            assertEquals(1, readStates.size());
            assertEquals(newLeaderId, readStates.getFirst().nodeId());
        }

        @Test
        void readIndexDuringPartitionFailsGracefully() {
            var network = Network.create(3);
            electLeader(network);

            String followerId = Network.nodeId(2);
            network.isolate(followerId);

            byte[] context = "partitioned-read".getBytes();
            network.readIndex(followerId, context);

            for (int i = 0; i < 10; i++) {
                network.tick();
            }

            var readStates = network.drainReadStates();
            assertTrue(readStates.isEmpty());
        }

        @Test
        void multipleReadIndexRequestsFromDifferentNodes() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();

            byte[] ctx1 = "read-from-leader".getBytes();
            byte[] ctx2 = "read-from-follower-1".getBytes();
            byte[] ctx3 = "read-from-follower-2".getBytes();

            network.readIndex(leaderId, ctx1);
            network.readIndex(Network.nodeId(1), ctx2);
            network.readIndex(Network.nodeId(2), ctx3);

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var readStates = network.drainReadStates();
            assertEquals(3, readStates.size());
        }
    }

    @Nested
    class Learners {
        @Test
        void learnerReplicatesWithCluster() {
            var network = Network.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.propose(leaderId, "hello".getBytes());

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var learner = network.node(3);
            var leader = network.node(leaderId);
            assertEquals(leader.commitIndex(), learner.commitIndex());
        }

        @Test
        void clusterCommitsWithOnlyVoterMajority() {
            var network = Network.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(Network.nodeId(3));

            network.propose(leaderId, "hello".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var leader = network.node(leaderId);
            assertTrue(leader.commitIndex() >= 2);
        }

        @Test
        void learnerDoesNotAffectQuorum() {
            var network = Network.create(2, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            long commitBeforeIsolation = leader.commitIndex();

            String otherVoter = leaderId.equals(Network.nodeId(0)) ? Network.nodeId(1) : Network.nodeId(0);
            network.isolate(otherVoter);

            network.propose(leaderId, "hello".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(commitBeforeIsolation, leader.commitIndex());
        }

        @Test
        void learnerCatchesUpWithCluster() {
            var network = Network.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(Network.nodeId(3));

            network.propose(leaderId, "entry1".getBytes());
            network.propose(leaderId, "entry2".getBytes());
            network.propose(leaderId, "entry3".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(Network.nodeId(3));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var learner = network.node(3);
            var leader = network.node(leaderId);
            assertEquals(leader.commitIndex(), learner.commitIndex());
        }

        @Test
        void learnerDoesNotBecomeLeader() {
            var network = Network.create(3, 1);
            electLeader(network);

            for (int i = 0; i < 100; i++) {
                network.tick();
                network.deliverAll();
            }

            var learner = network.node(3);
            assertFalse(learner.isLeader());
            assertEquals(Role.FOLLOWER, learner.role());
        }

        @Test
        void learnerReceivesSnapshotFromLeader() {
            var network = Network.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(Network.nodeId(3));

            for (int i = 0; i < 5; i++) {
                network.propose(leaderId, ("entry-" + i).getBytes());
            }

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var leader = network.node(leaderId);
            long commitBeforeCompaction = leader.commitIndex();
            assertTrue(commitBeforeCompaction >= 5);

            network.heal(Network.nodeId(3));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var learner = network.node(3);
            assertEquals(leader.commitIndex(), learner.commitIndex());
        }
    }

    @Nested
    class ConfigChanges {
        @Test
        void addLearnerReplicatesAfterCommit() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            network.proposeConfigChange(leaderId, new ConfigChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.membership().isLearner("node-3"));

            var membershipChanges = network.drainMembershipChanges();
            long changesWithNewLearner = membershipChanges.stream()
                .filter(c -> c.current().isLearner("node-3"))
                .count();
            assertTrue(changesWithNewLearner >= 1);
        }

        @Test
        void promotedVoterAffectsQuorum() {
            var network = Network.create(2, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            network.proposeConfigChange(leaderId, new ConfigChange.PromoteVoter(Network.nodeId(2)));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.membership().isVoter(Network.nodeId(2)));

            String otherVoter = leaderId.equals(Network.nodeId(0)) ? Network.nodeId(1) : Network.nodeId(0);
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
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);

            network.proposeConfigChange(leaderId, new ConfigChange.RemoveNode(leaderId));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(Role.FOLLOWER, leader.role());
        }

        @Test
        void configChangeCommitsWithMajority() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            network.isolate(Network.nodeId(2));

            long commitBefore = leader.commitIndex();
            network.proposeConfigChange(leaderId, new ConfigChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertTrue(leader.commitIndex() > commitBefore);
            assertTrue(leader.membership().isLearner("node-3"));
        }

        @Test
        void removedNodeStopsReceivingAppends() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var removedNodeId = Network.nodeId(2);
            var removedNode = network.node(removedNodeId);

            network.proposeConfigChange(leaderId, new ConfigChange.RemoveNode(removedNodeId));

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
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var demotedNodeId = Network.nodeId(2);

            network.proposeConfigChange(leaderId, new ConfigChange.DemoteToLearner(demotedNodeId));

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
            var network = Network.create(3);
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

            network.proposeConfigChange(leaderId, new ConfigChange.AddLearner("node-3"));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var storage = new InMemoryStorage(leader.membership());
            var newLearner = new Raft("node-3", leader.membership(), RaftConfig.defaults(), storage, _ -> 5);
            network.addNode("node-3", newLearner);

            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(leader.commitIndex(), newLearner.commitIndex());
        }

        @Test
        void configChangeEmitsMembershipChange() {
            var network = Network.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();

            network.proposeConfigChange(leaderId, new ConfigChange.AddLearner("node-3"));

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
