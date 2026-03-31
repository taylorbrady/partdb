package io.partdb.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedClusterSmokeTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(15);

    @TempDir
    Path tempDir;

    @Test
    void installedAppFormsClusterAndSurvivesLeaderFailover() throws Exception {
        Path installDir = Path.of(System.getProperty("partdb.app.installDir"));
        Path executable = installDir.resolve("bin").resolve("partdb-app");

        try (var cluster = PackagedClusterHarness.create(executable, tempDir, 3)) {
            cluster.startAll();

            var leader = cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            var putResult = cluster.runCommand(
                "kv",
                "put",
                "smoke-key",
                "smoke-value",
                "--endpoint",
                cluster.grpcAddress(leader.nodeId())
            );
            assertEquals(0, putResult.exitCode());
            assertEquals("OK\n", putResult.stdout());
            assertEquals("", putResult.stderr());

            var getResult = cluster.runCommand(
                "kv",
                "get",
                "smoke-key",
                "--endpoint",
                cluster.grpcAddress(leader.nodeId())
            );
            assertEquals(0, getResult.exitCode());
            assertEquals("smoke-value\n", getResult.stdout());
            assertEquals("", getResult.stderr());

            cluster.stopNode(leader.nodeId());

            var newLeader = cluster.awaitStableLeaderExcluding(leader.nodeId(), CLUSTER_TIMEOUT);

            var statusResult = cluster.runCommand(
                "cluster",
                "status",
                "--endpoint",
                cluster.grpcAddress(newLeader.nodeId())
            );
            assertEquals(0, statusResult.exitCode());
            assertEquals("", statusResult.stderr());
            assertTrue(statusResult.stdout().contains("Node ID:        " + newLeader.nodeId()));
            assertTrue(statusResult.stdout().contains("Role:           LEADER"));

            var memberListResult = cluster.runCommand(
                "cluster",
                "members",
                "--endpoint",
                cluster.grpcAddress(newLeader.nodeId())
            );
            assertEquals(0, memberListResult.exitCode());
            assertEquals("", memberListResult.stderr());
            assertTrue(memberListResult.stdout().contains("node1"));
            assertTrue(memberListResult.stdout().contains("node2"));
            assertTrue(memberListResult.stdout().contains("node3"));
            assertTrue(memberListResult.stdout().contains("leader"));

            var postFailoverPut = cluster.runCommand(
                "kv",
                "put",
                "failover-key",
                "failover-value",
                "--endpoint",
                cluster.grpcAddress(newLeader.nodeId())
            );
            assertEquals(
                0,
                postFailoverPut.exitCode(),
                "stdout:\n" + postFailoverPut.stdout() + "\nstderr:\n" + postFailoverPut.stderr()
            );
            assertEquals("OK\n", postFailoverPut.stdout(), "stderr:\n" + postFailoverPut.stderr());
            assertEquals("", postFailoverPut.stderr(), "stdout:\n" + postFailoverPut.stdout());

            var postFailoverGet = cluster.runCommand(
                "kv",
                "get",
                "failover-key",
                "--endpoint",
                cluster.grpcAddress(newLeader.nodeId())
            );
            assertEquals(
                0,
                postFailoverGet.exitCode(),
                "stdout:\n" + postFailoverGet.stdout() + "\nstderr:\n" + postFailoverGet.stderr()
            );
            assertEquals("failover-value\n", postFailoverGet.stdout(), "stderr:\n" + postFailoverGet.stderr());
            assertEquals("", postFailoverGet.stderr(), "stdout:\n" + postFailoverGet.stdout());
        }
    }
}
