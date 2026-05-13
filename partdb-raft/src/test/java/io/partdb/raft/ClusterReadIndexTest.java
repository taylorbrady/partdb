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

class ClusterReadIndexTest extends ClusterTestSupport {

    @Nested
    class ReadIndexRequested {
        @Test
        void leaderServesReadIndexDirectly() {
            var network = ClusterHarness.create(3);
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
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            String followerId = ClusterHarness.nodeId(0).equals(leaderId) ? ClusterHarness.nodeId(1) : ClusterHarness.nodeId(0);

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
            var network = ClusterHarness.create(3);
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
            var network = ClusterHarness.create(3);
            electLeader(network);

            String followerId = ClusterHarness.nodeId(2);
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
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();

            byte[] ctx1 = "read-from-leader".getBytes();
            byte[] ctx2 = "read-from-follower-1".getBytes();
            byte[] ctx3 = "read-from-follower-2".getBytes();

            network.readIndex(leaderId, ctx1);
            network.readIndex(ClusterHarness.nodeId(1), ctx2);
            network.readIndex(ClusterHarness.nodeId(2), ctx3);

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var readStates = network.drainReadStates();
            assertEquals(3, readStates.size());
        }
    }
}
