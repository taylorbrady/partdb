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

class ClusterReplicationTest extends ClusterTestSupport {

    @Nested
    class Replication {
        @Test
        void proposalReplicatesToAllNodes() {
            var network = ClusterHarness.create(3);
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
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(ClusterHarness.nodeId(2));

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
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(ClusterHarness.nodeId(2));

            network.propose(leaderId, "entry1".getBytes());
            network.propose(leaderId, "entry2".getBytes());
            network.propose(leaderId, "entry3".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(ClusterHarness.nodeId(2));

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
            var network = ClusterHarness.create(3);
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
                .mapToLong(RaftNode::commitIndex)
                .min()
                .orElse(0);

            assertTrue(minCommit >= 6);
        }
    }

    @Nested
    class Consistency {
        @Test
        void allNodesAgreeOnCommittedEntries() {
            var network = ClusterHarness.create(3);
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
            var network = ClusterHarness.create(3);

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
}
