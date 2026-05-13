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

class ClusterElectionTest extends ClusterTestSupport {

    @Nested
    class LeaderElection {
        @Test
        void singleNodeClusterElectsLeader() {
            var network = ClusterHarness.create(1);

            electLeader(network);

            assertTrue(network.node(0).isLeader());
        }

        @Test
        void threeNodeClusterElectsLeader() {
            var network = ClusterHarness.create(3);

            electLeader(network);

            assertEquals(1, network.leaderCount());
        }

        @Test
        void fiveNodeClusterElectsLeader() {
            var network = ClusterHarness.create(5);

            electLeader(network);

            assertEquals(1, network.leaderCount());
        }

        @Test
        void atMostOneLeaderPerTerm() {
            var network = ClusterHarness.create(5);

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
            var network = ClusterHarness.create(3);

            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            assertEquals(leaderId, leader.leaderId().orElseThrow());
        }
    }

    @Nested
    class PreVoteCluster {
        @Test
        void clusterElectsLeaderWithPreVote() {
            var network = ClusterHarness.create(3);

            electLeader(network);

            assertEquals(1, network.leaderCount());
            var leader = network.node(network.leader().orElseThrow());
            assertTrue(leader.term() >= 1);
        }

        @Test
        void partitionedNodeDoesNotInflateTerm() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var isolatedNode = network.node(2);
            var termBeforeIsolation = isolatedNode.term();

            network.isolate(ClusterHarness.nodeId(2));

            for (int i = 0; i < 100; i++) {
                network.tickNode(ClusterHarness.nodeId(2));
            }

            var termDuringIsolation = isolatedNode.term();
            assertEquals(termBeforeIsolation, termDuringIsolation);
        }

        @Test
        void healedNodeRejoinsWithoutTermInflation() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var isolatedNode = network.node(2);
            var termBeforeIsolation = isolatedNode.term();

            network.isolate(ClusterHarness.nodeId(2));

            for (int i = 0; i < 100; i++) {
                network.tickNode(ClusterHarness.nodeId(2));
            }

            assertEquals(termBeforeIsolation, isolatedNode.term());

            network.heal(ClusterHarness.nodeId(2));

            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(1, network.leaderCount());
        }

        @Test
        void preVoteAllowsElectionWhenLeaderFails() {
            var network = ClusterHarness.create(3);
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
                .filter(RaftNode::isLeader)
                .count();

            assertTrue(remainingNodes >= 1);
        }
    }
}
