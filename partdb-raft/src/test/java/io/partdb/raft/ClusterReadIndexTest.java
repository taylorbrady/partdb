package io.partdb.raft;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;


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

            long context = 1L;
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
            assertEquals(context, readState.context());
        }

        @Test
        void followerForwardsReadIndexToLeader() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            String followerId = ClusterHarness.nodeId(0).equals(leaderId) ? ClusterHarness.nodeId(1) : ClusterHarness.nodeId(0);

            long context = 2L;
            network.readIndex(followerId, context);

            for (int i = 0; i < 20; i++) {
                network.tick();
                network.deliverAll();
            }

            var readStates = network.drainReadStates();
            assertEquals(1, readStates.size());

            var readState = readStates.getFirst();
            assertEquals(followerId, readState.nodeId());
            assertEquals(context, readState.context());
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

            long context = 3L;
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

            long context = 4L;
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

            long ctx1 = 5L;
            long ctx2 = 6L;
            long ctx3 = 7L;

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
