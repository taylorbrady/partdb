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

class ClusterSnapshotTest extends ClusterTestSupport {

    @Nested
    class SnapshotRecovery {
        @Test
        void laggedFollowerReceivesSnapshotWhenLogCompacted() {
            var network = ClusterHarness.create(3);
            electLeader(network);

            var leaderId = network.leader().orElseThrow();
            String laggedFollowerId = ClusterHarness.nodeId(2);
            network.isolate(laggedFollowerId);

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

            Bytes snapshotData = Bytes.utf8("snapshot-at-" + commitBeforeCompaction);
            network.compactWithSnapshot(leaderId, commitBeforeCompaction, snapshotData);

            network.heal(laggedFollowerId);

            for (int i = 0; i < 30; i++) {
                network.tick();
                network.deliverAll();
            }

            var lagged = network.node(laggedFollowerId);
            assertEquals(leader.commitIndex(), lagged.commitIndex());
            var installedSnapshot = network.snapshot(laggedFollowerId).orElseThrow();
            assertEquals(commitBeforeCompaction, installedSnapshot.index());
            assertEquals(snapshotData, installedSnapshot.data());
        }
    }
}
