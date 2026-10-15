package io.partdb.app;

import io.partdb.client.ClusterMember;
import io.partdb.client.ClusterMemberRole;
import io.partdb.client.ClusterNodeRole;
import io.partdb.client.testing.TestCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterAdminIntegrationTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(10);

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Test
    void statusAndMembershipConvergeAcrossAllNodes() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            String leaderId = cluster.awaitStableLeader(CLUSTER_TIMEOUT).nodeId();

            for (String nodeId : cluster.runningNodeIds()) {
                var status = cluster.awaitStatusLeader(nodeId, leaderId, CLUSTER_TIMEOUT);
                assertTrue(status.running());
                assertEquals(nodeId.equals(leaderId), status.role() == ClusterNodeRole.LEADER);

                var membership = cluster.awaitMembershipLeader(nodeId, leaderId, CLUSTER_TIMEOUT);
                assertEquals(3, membership.members().size());
                assertEquals(leaderId, membership.leaderId().orElseThrow());

                for (ClusterMember member : membership.members()) {
                    assertEquals(ClusterMemberRole.VOTER, member.role());
                    assertEquals(member.nodeId().equals(nodeId), member.self());
                    assertEquals(member.nodeId().equals(leaderId), member.leader());
                }
            }
        }
    }

    @Test
    void statusAndMemberListCommandsReflectLeaderAfterFailover() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();

            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.stopNode(initialLeader.nodeId());

            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            assertNotEquals(initialLeader.nodeId(), newLeader.nodeId());
            String observerId = cluster.runningNodeIds().stream()
                .filter(nodeId -> !nodeId.equals(newLeader.nodeId()))
                .findFirst()
                .orElseThrow();

            cluster.awaitStatusLeader(observerId, newLeader.nodeId(), CLUSTER_TIMEOUT);
            cluster.awaitMembershipLeader(observerId, newLeader.nodeId(), CLUSTER_TIMEOUT);

            var statusResult = InProcessCli.run("cluster", "status", "--endpoint", cluster.grpcAddress(observerId));
            assertEquals(0, statusResult.exitCode(), statusResult.stderr());
            assertEquals("", statusResult.stderr());
            assertTrue(statusResult.stdout().contains("Node ID:        " + observerId), statusResult.stdout());
            assertTrue(statusResult.stdout().contains("Leader:         " + newLeader.nodeId()), statusResult.stdout());

            var membersResult = InProcessCli.run(
                "cluster",
                "members",
                "--endpoint",
                cluster.grpcAddress(observerId),
                "--output",
                "json"
            );
            assertEquals(0, membersResult.exitCode(), membersResult.stderr());
            assertEquals("", membersResult.stderr());
            String json = membersResult.stdout();
            assertTrue(json.contains("\"leaderId\":\"" + newLeader.nodeId() + "\""), json);
            assertTrue(json.contains("\"nodeId\":\"" + initialLeader.nodeId() + "\""), json);
            assertTrue(json.contains("\"nodeId\":\"" + newLeader.nodeId() + "\""), json);
            assertEquals(1, json.split("\"isLeader\":true", -1).length - 1, json);
        }
    }

    private TestCluster<InProcessNode> newCluster() throws IOException {
        return TestCluster.create(3, spec -> new InProcessNode(tempDir, spec));
    }
}
