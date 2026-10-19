package io.partdb.app;

import io.partdb.app.testing.CommandResult;
import io.partdb.app.testing.TestCluster;
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
    void cliCommandsWorkAgainstALiveSingleNodeServer() throws Exception {
        try (var cluster = TestCluster.create(1, spec -> new InProcessNode(tempDir, spec))) {
            cluster.startAll();
            var leader = cluster.awaitStableLeader(LEADER_TIMEOUT);
            String endpoint = cluster.endpoint(leader.nodeId());

            var put = InProcessCli.run("kv", "put", "hello", "world", "--endpoint", endpoint);
            assertEquals(0, put.exitCode(), put.stderr());
            assertTrue(put.stdout().startsWith("OK revision="), put.stdout());
            assertOutput(InProcessCli.run("kv", "get", "hello", "--endpoint", endpoint), "world\n");
            assertOutput(InProcessCli.run("kv", "put", "help", "me", "--endpoint", endpoint, "--output", "json"), null);
            assertOutput(InProcessCli.run("kv", "scan", "he", "hf", "--endpoint", endpoint), "hello=world\nhelp=me\n");
            assertOutput(InProcessCli.run("kv", "scan", "he", "hf", "--limit", "1", "--endpoint", endpoint), "hello=world\n");
            var scanned = InProcessCli.run("kv", "scan", "he", "hf", "--endpoint", endpoint, "--output", "json");
            assertEquals(0, scanned.exitCode(), scanned.stderr());
            assertTrue(scanned.stdout().startsWith("{\"readRevision\":"), scanned.stdout());
            assertTrue(scanned.stdout().contains("{\"key\":\"hello\",\"value\":\"world\""), scanned.stdout());
            assertTrue(scanned.stdout().contains("{\"key\":\"help\",\"value\":\"me\""), scanned.stdout());

            var conflict = InProcessCli.run("kv", "put", "hello", "again", "--if-missing", "--endpoint", endpoint);
            assertEquals(ExitCode.CONDITION_FAILED.code(), conflict.exitCode());
            assertEquals("Error: condition failed\n", conflict.stderr());
            long revision = Long.parseLong(put.stdout().strip().substring("OK revision=".length()));
            assertOutput(InProcessCli.run("kv", "put", "hello", "world", "--if-revision", Long.toString(revision), "--endpoint", endpoint), null);
            assertEquals(ExitCode.CONDITION_FAILED.code(),
                InProcessCli.run("kv", "delete", "hello", "--if-revision", Long.toString(revision), "--endpoint", endpoint).exitCode());

            var delete = InProcessCli.run("kv", "delete", "hello", "--endpoint", endpoint);
            assertEquals(0, delete.exitCode(), delete.stderr());
            assertEquals(ExitCode.NOT_FOUND.code(), InProcessCli.run("kv", "get", "hello", "--endpoint", endpoint).exitCode());

            var status = InProcessCli.run("cluster", "status", "--endpoint", endpoint);
            assertEquals(0, status.exitCode(), status.stderr());
            assertTrue(status.stdout().contains("Node ID:        " + leader.nodeId()), status.stdout());
            assertTrue(status.stdout().contains("Role:           LEADER"), status.stdout());

            var members = InProcessCli.run("cluster", "members", "--endpoint", endpoint);
            assertEquals(0, members.exitCode(), members.stderr());
            assertTrue(members.stdout().contains(leader.nodeId()), members.stdout());
            assertTrue(members.stdout().contains("leader"), members.stdout());
        }
    }

    private static void assertOutput(CommandResult result, String expected) {
        assertEquals(0, result.exitCode(), result.stderr());
        if (expected != null) {
            assertEquals(expected, result.stdout());
        }
        assertEquals("", result.stderr());
    }
}
