package io.partdb.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerMetricsTest {

    private static final Pattern LINE = Pattern.compile("(# (HELP|TYPE) .+|[a-z_]+(\\{[a-z_]+=\"[^\"]*\"})? -?[0-9.eE+-]+)");

    @TempDir
    Path tempDir;

    @Test
    void servesPrometheusMetricsForTheNodeAndItsStorage() throws Exception {
        int adminPort = freePort();
        var config = ServerConfig.builder("node1", tempDir.resolve("node1")).clientPort(freePort()).adminPort(adminPort).build();

        try (var _ = PartDbServer.start(config)) {
            var response = awaitScrape(adminPort, "partdb_raft_role{role=\"leader\"} 1");

            assertEquals(PrometheusMetrics.CONTENT_TYPE, response.headers().firstValue("Content-Type").orElseThrow());
            String body = response.body();
            assertTrue(body.contains("partdb_node_info{node_id=\"node1\"} 1\n"), body);
            assertTrue(body.contains("partdb_node_running 1\n"), body);
            assertTrue(body.contains("partdb_raft_has_leader 1\n"), body);
            assertTrue(body.contains("# TYPE partdb_proposals_total counter\n"), body);
            assertTrue(body.contains("partdb_storage_tables 0\n"), body);
            body.lines().forEach(line -> assertTrue(LINE.matcher(line).matches(), "not valid exposition: " + line));
        }
    }

    private static HttpResponse<String> awaitScrape(int port, String expected) throws Exception {
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics")).timeout(Duration.ofSeconds(2)).GET().build();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        HttpResponse<String> response = null;
        while (System.nanoTime() < deadline) {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200 && response.body().contains(expected)) {
                return response;
            }
            Thread.sleep(Duration.ofMillis(10));
        }
        throw new AssertionError("metrics never contained " + expected + (response == null ? "" : ":\n" + response.body()));
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
