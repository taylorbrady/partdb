package io.partdb.app;

import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterClientConfig;
import io.partdb.client.ClusterNodeRole;
import io.partdb.client.ServerEndpoint;
import io.partdb.server.PartDbServer;
import io.partdb.server.PartDbServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveServerSmokeTest {
    @TempDir
    Path tempDir;

    @Test
    void cliCommandsWorkAgainstLiveSingleNodeServer() throws Exception {
        int raftPort = freePort();
        int grpcPort = freePort();
        int adminPort = freePort();
        var endpoint = "localhost:" + grpcPort;

        var config = PartDbServerConfig.create(
            "node1",
            Map.of(),
            tempDir.resolve("node1"),
            raftPort,
            grpcPort,
            adminPort
        );

        try (var server = new PartDbServer(config)) {
            server.start();
            awaitLeader(grpcPort);

            assertCommand(new String[] {"kv", "put", "hello", "world", "--endpoint", endpoint}, 0, "OK\n");
            assertCommand(new String[] {"kv", "get", "hello", "--endpoint", endpoint}, 0, "world\n");

            var statusResult = runCommand("cluster", "status", "--endpoint", endpoint);
            assertEquals(0, statusResult.exitCode());
            assertTrue(statusResult.stdout().contains("Node ID:        node1"));
            assertTrue(statusResult.stdout().contains("Role:           LEADER"));

            var memberResult = runCommand("cluster", "members", "--endpoint", endpoint);
            assertEquals(0, memberResult.exitCode());
            assertTrue(memberResult.stdout().contains("node1"));
            assertTrue(memberResult.stdout().contains("localhost:" + raftPort));
            assertTrue(memberResult.stdout().contains("leader"));
        }
    }

    private void awaitLeader(int grpcPort) throws Exception {
        var config = ClusterClientConfig.defaultConfig(new ServerEndpoint("localhost", grpcPort));
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try (var client = new ClusterClient(config)) {
                var status = client.status().get();
                if (status.role() == ClusterNodeRole.LEADER && status.running()) {
                    return;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(50L);
        }

        throw new AssertionError("Timed out waiting for leader", lastFailure);
    }

    private void assertCommand(String[] args, int expectedExitCode, String expectedStdout) {
        var result = runCommand(args);
        assertEquals(expectedExitCode, result.exitCode());
        assertEquals(expectedStdout, result.stdout());
        assertEquals("", result.stderr());
    }

    private CommandResult runCommand(String... args) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        try (
            PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
            PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
        ) {
            int exitCode = PartDbApp.run(args, out, err);
            return new CommandResult(
                exitCode,
                outBytes.toString(StandardCharsets.UTF_8),
                errBytes.toString(StandardCharsets.UTF_8)
            );
        }
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record CommandResult(int exitCode, String stdout, String stderr) {}
}
