package io.partdb.app;

import io.partdb.client.ClusterMember;
import io.partdb.client.ClusterMemberRole;
import io.partdb.client.ClusterMembership;
import io.partdb.client.ClusterNodeRole;
import io.partdb.client.ClusterStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterAdminIntegrationTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    Path tempDir;

    @Test
    void statusAndMembershipConvergeAcrossAllNodes() throws Exception {
        try (var cluster = MultiNodeClusterHarness.create(tempDir, 3)) {
            cluster.startAll();

            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String leaderId = leader.nodeId();

            for (String nodeId : cluster.runningNodeIds()) {
                ClusterStatus status = cluster.awaitStatusLeader(nodeId, leaderId, CLUSTER_TIMEOUT);
                assertTrue(status.running());
                if (nodeId.equals(leaderId)) {
                    assertEquals(ClusterNodeRole.LEADER, status.role());
                } else {
                    assertFalse(status.role() == ClusterNodeRole.LEADER);
                }

                ClusterMembership membership = cluster.awaitMembershipLeader(nodeId, leaderId, CLUSTER_TIMEOUT);
                assertEquals(3, membership.members().size());
                assertEquals(leaderId, membership.leaderId().orElseThrow());
                assertEquals(1L, membership.members().stream().filter(ClusterMember::leader).count());

                var members = membership.members().stream()
                    .sorted(Comparator.comparing(ClusterMember::nodeId))
                    .toList();

                for (ClusterMember member : members) {
                    assertEquals(ClusterMemberRole.VOTER, member.role());
                    assertEquals(member.nodeId().equals(nodeId), member.self());
                    assertEquals(member.nodeId().equals(leaderId), member.leader());
                }
            }
        }
    }

    @Test
    void statusAndMemberListCommandsReflectLeaderAfterFailover() throws Exception {
        try (var cluster = MultiNodeClusterHarness.create(tempDir, 3)) {
            cluster.startAll();

            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.stopNode(initialLeader.nodeId());

            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            String observerId = cluster.runningNodeIds().stream()
                .filter(nodeId -> !nodeId.equals(newLeader.nodeId()))
                .findFirst()
                .orElse(newLeader.nodeId());

            cluster.awaitStatusLeader(observerId, newLeader.nodeId(), CLUSTER_TIMEOUT);
            cluster.awaitMembershipLeader(observerId, newLeader.nodeId(), CLUSTER_TIMEOUT);

            var statusResult = cluster.runCommand("cluster", "status", "--endpoint", cluster.grpcAddress(observerId));
            assertEquals(0, statusResult.exitCode());
            assertEquals("", statusResult.stderr());
            assertTrue(statusResult.stdout().contains("Node ID:        " + observerId));
            assertTrue(statusResult.stdout().contains("Leader:         " + newLeader.nodeId()));

            var membersResult = cluster.runCommand(
                "cluster",
                "members",
                "--endpoint",
                cluster.grpcAddress(observerId),
                "--output",
                "json"
            );
            assertEquals(0, membersResult.exitCode());
            assertEquals("", membersResult.stderr());
            assertTrue(membersResult.stdout().contains("\"leaderId\":\"" + newLeader.nodeId() + "\""));
            assertTrue(membersResult.stdout().contains("\"nodeId\":\"" + initialLeader.nodeId() + "\""));
            assertTrue(membersResult.stdout().contains("\"nodeId\":\"" + newLeader.nodeId() + "\""));
            assertEquals(1, countOccurrences(membersResult.stdout(), "\"isLeader\":true"));
        }
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(token, index)) >= 0) {
            count++;
            index += token.length();
        }
        return count;
    }
}
