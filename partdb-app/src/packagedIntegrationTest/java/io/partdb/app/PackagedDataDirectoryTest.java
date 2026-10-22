package io.partdb.app;

import io.partdb.app.testing.TestCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedDataDirectoryTest {
    private static final Duration CLUSTER_TIMEOUT = Duration.ofSeconds(15);

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path tempDir;

    @Test
    void secondServerOnALiveDataDirectoryRefusesToStart() throws Exception {
        var app = PackagedApp.fromInstallDir();

        try (var cluster = TestCluster.create(1, spec -> app.newNode(tempDir, spec))) {
            cluster.startAll();
            var node = cluster.awaitStableLeader(CLUSTER_TIMEOUT);

            var second = app.run("server", "start",
                "--node-id", node.nodeId(),
                "--data-dir", node.dataDirectory().toString(),
                "--client-port", Integer.toString(freePort()),
                "--admin-port", Integer.toString(freePort()));

            assertEquals(1, second.exitCode(), second.stdout() + second.stderr());
            assertTrue(second.stdout().contains("is already in use"), second.stdout());
            assertEquals(node.nodeId(), cluster.status(node.nodeId()).nodeId());
        }
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
