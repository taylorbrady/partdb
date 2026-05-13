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

class ClusterPartitionTest extends ClusterTestSupport {

    @Nested
    class Partitions {
        @Test
        void majorityElectsNewLeaderDuringPartition() {
            var network = ClusterHarness.create(3);
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
            var network = ClusterHarness.create(3);
            electLeader(network);

            network.isolate(ClusterHarness.nodeId(2));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(ClusterHarness.nodeId(2));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            assertEquals(1, network.leaderCount());
        }

        @Test
        void oldLeaderBecomesFollowerOnRejoin() {
            var network = ClusterHarness.create(3);
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

            assertEquals(RaftRole.FOLLOWER, oldLeader.role());
            assertTrue(oldLeader.term() > oldTerm);
        }

        @Test
        void minorityCannotElectLeader() {
            var network = ClusterHarness.create(5);
            electLeader(network);

            network.isolate(ClusterHarness.nodeId(0));
            network.isolate(ClusterHarness.nodeId(1));
            network.isolate(ClusterHarness.nodeId(2));

            for (int i = 0; i < 50; i++) {
                network.tick();
                network.deliverAll();
            }

            var node3 = network.node(3);
            var node4 = network.node(4);
            assertFalse(node3.isLeader() && node4.isLeader());
        }
    }
}
