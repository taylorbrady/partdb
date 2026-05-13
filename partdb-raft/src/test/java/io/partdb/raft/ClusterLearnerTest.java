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

class ClusterLearnerTest extends ClusterTestSupport {

    @Nested
    class Learners {
        @Test
        void learnerReplicatesWithCluster() {
            var network = ClusterHarness.create(3, 1);
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
            var network = ClusterHarness.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(ClusterHarness.nodeId(3));

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
            var network = ClusterHarness.create(2, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            var leader = network.node(leaderId);
            long commitBeforeIsolation = leader.commitIndex();

            String otherVoter = leaderId.equals(ClusterHarness.nodeId(0)) ? ClusterHarness.nodeId(1) : ClusterHarness.nodeId(0);
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
            var network = ClusterHarness.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(ClusterHarness.nodeId(3));

            network.propose(leaderId, "entry1".getBytes());
            network.propose(leaderId, "entry2".getBytes());
            network.propose(leaderId, "entry3".getBytes());

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            network.heal(ClusterHarness.nodeId(3));

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
            var network = ClusterHarness.create(3, 1);
            electLeader(network);

            for (int i = 0; i < 100; i++) {
                network.tick();
                network.deliverAll();
            }

            var learner = network.node(3);
            assertFalse(learner.isLeader());
            assertEquals(RaftRole.FOLLOWER, learner.role());
        }

        @Test
        void learnerReceivesSnapshotFromLeader() {
            var network = ClusterHarness.create(3, 1);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            network.isolate(ClusterHarness.nodeId(3));

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

            network.heal(ClusterHarness.nodeId(3));

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var learner = network.node(3);
            assertEquals(leader.commitIndex(), learner.commitIndex());
        }
    }
}
