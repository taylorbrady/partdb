package io.partdb.app;

import io.partdb.client.testing.CommandResult;
import io.partdb.client.testing.TestCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedClusterSmokeTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(15);

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Test
    void installedAppFormsClusterServesCliCommandsAndStopsCleanly() throws Exception {
        var app = PackagedApp.fromInstallDir();

        try (var cluster = TestCluster.create(3, spec -> app.newNode(tempDir, spec))) {
            cluster.startAll();

            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);
            String endpoint = cluster.grpcAddress(leader.nodeId());

            assertCommand(app.run("kv", "put", "smoke-key", "smoke-value", "--endpoint", endpoint), "OK\n");
            assertCommand(app.run("kv", "get", "smoke-key", "--endpoint", endpoint), "smoke-value\n");

            var statusResult = app.run("cluster", "status", "--endpoint", endpoint);
            assertEquals(0, statusResult.exitCode(), statusResult.stderr());
            assertEquals("", statusResult.stderr());
            assertTrue(statusResult.stdout().contains("Node ID:        " + leader.nodeId()), statusResult.stdout());
            assertTrue(statusResult.stdout().contains("Role:           LEADER"), statusResult.stdout());

            var membersResult = app.run("cluster", "members", "--endpoint", endpoint);
            assertEquals(0, membersResult.exitCode(), membersResult.stderr());
            assertEquals("", membersResult.stderr());
            for (String nodeId : cluster.runningNodeIds()) {
                assertTrue(membersResult.stdout().contains(nodeId), membersResult.stdout());
            }
            assertTrue(membersResult.stdout().contains("leader"), membersResult.stdout());
        }
    }

    private static void assertCommand(CommandResult result, String expectedStdout) {
        assertEquals(0, result.exitCode(), result.stderr());
        assertEquals(expectedStdout, result.stdout());
        assertEquals("", result.stderr());
    }
}
