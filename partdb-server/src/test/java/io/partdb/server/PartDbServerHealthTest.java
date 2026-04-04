package io.partdb.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbServerHealthTest {
    private static final Duration HEALTH_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(25);

    @TempDir
    Path tempDir;

    @Test
    void livezAndReadyzReportHealthySingleNodeLeader() throws Exception {
        int raftPort = freePort();
        int grpcPort = freePort();
        int adminPort = freePort();
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

            awaitHealth(adminPort, "/livez", 200, "ok\n");
            awaitHealth(adminPort, "/readyz", 200, "ok\n");
        }
    }

    @Test
    void readyzReportsServiceUnavailableWithoutKnownLeader() throws Exception {
        int raftPort = freePort();
        int grpcPort = freePort();
        int adminPort = freePort();
        var config = PartDbServerConfig.create(
            "node1",
            Map.of(
                "node1", "localhost:" + raftPort,
                "node2", "localhost:" + freePort(),
                "node3", "localhost:" + freePort()
            ),
            tempDir.resolve("node1"),
            raftPort,
            grpcPort,
            adminPort
        );

        try (var server = new PartDbServer(config)) {
            server.start();

            awaitHealth(adminPort, "/livez", 200, "ok\n");
            HealthResponse response = awaitHealth(adminPort, "/readyz", 503, null);
            assertTrue(response.body().startsWith("not ready: "));
        }
    }

    private static HealthResponse awaitHealth(int port, String path, int expectedStatus, String expectedBody) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(2))
            .GET()
            .build();
        long deadlineNanos = System.nanoTime() + HEALTH_TIMEOUT.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                var health = new HealthResponse(response.statusCode(), response.body());
                if (health.statusCode() == expectedStatus
                    && (expectedBody == null || expectedBody.equals(health.body()))) {
                    return health;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError("Timed out waiting for " + path + " to return " + expectedStatus, lastFailure);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record HealthResponse(int statusCode, String body) {}
}
