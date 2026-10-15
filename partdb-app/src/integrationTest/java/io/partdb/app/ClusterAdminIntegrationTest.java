package io.partdb.app;

import io.partdb.app.testing.TestCluster;
import io.partdb.client.MemberRole;
import io.partdb.client.Role;
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
    void statusAndMembersConvergeAcrossAllNodes() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            String leaderId = cluster.awaitStableLeader(CLUSTER_TIMEOUT).nodeId();

            for (String nodeId : cluster.startedNodeIds()) {
                var status = cluster.awaitStatusLeader(nodeId, leaderId, CLUSTER_TIMEOUT);
                assertEquals(nodeId, status.nodeId());
                assertEquals(nodeId.equals(leaderId), status.role() == Role.LEADER);

                var members = cluster.awaitMembersLeader(nodeId, leaderId, CLUSTER_TIMEOUT);
                assertEquals(3, members.members().size());
                assertTrue(members.members().stream().allMatch(member -> member.role() == MemberRole.VOTER));
            }
        }
    }

    @Test
    void cliReflectsTheLeaderAfterFailover() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            var initialLeader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            cluster.stopNode(initialLeader.nodeId());
            var newLeader = cluster.awaitStableLeaderExcluding(initialLeader.nodeId(), CLUSTER_TIMEOUT);
            assertNotEquals(initialLeader.nodeId(), newLeader.nodeId());
            String observer = cluster.startedNodeIds().stream().filter(id -> !id.equals(newLeader.nodeId())).findFirst().orElseThrow();
            cluster.awaitStatusLeader(observer, newLeader.nodeId(), CLUSTER_TIMEOUT);

            var status = InProcessCli.run("cluster", "status", "--endpoint", cluster.endpoint(observer));
            assertEquals(0, status.exitCode(), status.stderr());
            assertTrue(status.stdout().contains("Node ID:        " + observer), status.stdout());
            assertTrue(status.stdout().contains("Leader:         " + newLeader.nodeId()), status.stdout());

            var members = InProcessCli.run("cluster", "members", "--endpoint", cluster.endpoint(observer), "--output", "json");
            assertEquals(0, members.exitCode(), members.stderr());
            String json = members.stdout();
            assertTrue(json.contains("\"leaderId\":\"" + newLeader.nodeId() + "\""), json);
            assertTrue(json.contains("\"nodeId\":\"" + initialLeader.nodeId() + "\""), json);
            assertEquals(1, json.split("\"isLeader\":true", -1).length - 1, json);
        }
    }

    @Test
    void cliWritesFollowTheLeaderFromAFollowerEndpoint() throws Exception {
        try (var cluster = newCluster()) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String follower = cluster.startedNodeIds().stream().filter(id -> !id.equals(leader.nodeId())).findFirst().orElseThrow();

            var put = InProcessCli.run("kv", "put", "key", "value", "--endpoints", cluster.endpoints());
            assertEquals(0, put.exitCode(), put.stderr());
            assertTrue(put.stdout().startsWith("OK revision="), put.stdout());

            var get = InProcessCli.run("kv", "get", "key", "--endpoint", cluster.endpoint(follower));
            assertEquals(0, get.exitCode(), get.stderr());
            assertEquals("value\n", get.stdout());

            var missing = InProcessCli.run("kv", "get", "missing", "--endpoint", cluster.endpoint(follower));
            assertEquals(AppCommand.NOT_FOUND, missing.exitCode());
        }
    }

    private TestCluster<InProcessNode> newCluster() throws IOException {
        return TestCluster.create(3, spec -> new InProcessNode(tempDir, spec));
    }
}
