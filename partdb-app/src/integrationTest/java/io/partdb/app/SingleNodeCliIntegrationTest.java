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

class SingleNodeCliIntegrationTest {
    private static final Duration LEADER_TIMEOUT = Duration.ofSeconds(5);

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Test
    void cliCommandsWorkAgainstLiveSingleNodeServer() throws Exception {
        try (var cluster = TestCluster.create(1, spec -> new InProcessNode(tempDir, spec))) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(LEADER_TIMEOUT);
            String endpoint = cluster.grpcAddress(leader.nodeId());

            assertCommand(InProcessCli.run("kv", "put", "hello", "world", "--endpoint", endpoint), "OK\n");
            assertCommand(InProcessCli.run("kv", "get", "hello", "--endpoint", endpoint), "world\n");

            var statusResult = InProcessCli.run("cluster", "status", "--endpoint", endpoint);
            assertEquals(0, statusResult.exitCode(), statusResult.stderr());
            assertTrue(statusResult.stdout().contains("Node ID:        " + leader.nodeId()), statusResult.stdout());
            assertTrue(statusResult.stdout().contains("Role:           LEADER"), statusResult.stdout());

            var memberResult = InProcessCli.run("cluster", "members", "--endpoint", endpoint);
            assertEquals(0, memberResult.exitCode(), memberResult.stderr());
            assertTrue(memberResult.stdout().contains(leader.nodeId()), memberResult.stdout());
            assertTrue(memberResult.stdout().contains("leader"), memberResult.stdout());
        }
    }

    private static void assertCommand(CommandResult result, String expectedStdout) {
        assertEquals(0, result.exitCode(), result.stderr());
        assertEquals(expectedStdout, result.stdout());
        assertEquals("", result.stderr());
    }
}
