package io.partdb.transport.grpc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.partdb.node.PartDbNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class AdminHttpServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor;
    private final PartDbNode node;

    AdminHttpServer(PartDbNode node, int port) throws IOException {
        this.node = Objects.requireNonNull(node, "node must not be null");
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.server.setExecutor(executor);
        this.server.createContext("/livez", exchange -> handle(exchange, liveStatus()));
        this.server.createContext("/readyz", exchange -> handle(exchange, readyStatus()));
    }

    void start() {
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
    }

    private void handle(HttpExchange exchange, HealthStatus status) throws IOException {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                writeResponse(exchange, 405, "method not allowed\n");
                return;
            }
            writeResponse(exchange, status.code(), status.body());
        }
    }

    private HealthStatus liveStatus() {
        if (node.status().running()) {
            return HealthStatus.ok();
        }
        return HealthStatus.unavailable("not live: node stopped\n");
    }

    private HealthStatus readyStatus() {
        var status = node.status();
        if (!status.running()) {
            return HealthStatus.unavailable("not ready: node stopped\n");
        }
        return switch (status.role()) {
            case LEADER -> HealthStatus.ok();
            case FOLLOWER -> status.leaderId().isPresent()
                ? HealthStatus.ok()
                : HealthStatus.unavailable("not ready: leader unknown\n");
            case PRE_CANDIDATE, CANDIDATE -> HealthStatus.unavailable("not ready: role=" + status.role() + "\n");
        };
    }

    private static void writeResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private record HealthStatus(int code, String body) {
        private static HealthStatus ok() {
            return new HealthStatus(200, "ok\n");
        }

        private static HealthStatus unavailable(String body) {
            return new HealthStatus(503, body);
        }
    }
}
