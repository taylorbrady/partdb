package io.partdb.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.partdb.node.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

final class AdminHttpServer implements AutoCloseable {
    private static final String TEXT = "text/plain; charset=utf-8";

    private final HttpServer server;
    private final ExecutorService executor;
    private final Node node;
    private final PrometheusMetrics metrics;
    private volatile boolean draining;

    AdminHttpServer(Node node, int port) throws IOException {
        this.node = Objects.requireNonNull(node, "node must not be null");
        this.metrics = new PrometheusMetrics(node);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.server.setExecutor(executor);
        this.server.createContext("/livez", exchange -> handle(exchange, this::liveStatus));
        this.server.createContext("/readyz", exchange -> handle(exchange, this::readyStatus));
        this.server.createContext("/metrics", exchange -> handle(exchange, () -> new Response(200, PrometheusMetrics.CONTENT_TYPE, metrics.scrape())));
    }

    void start() {
        server.start();
    }

    void drain() {
        draining = true;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
    }

    private void handle(HttpExchange exchange, Supplier<Response> response) throws IOException {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                write(exchange, new Response(405, TEXT, "method not allowed\n"));
                return;
            }
            write(exchange, response.get());
        }
    }

    private Response liveStatus() {
        if (node.status().running()) {
            return Response.ok();
        }
        return Response.unavailable("not live: node stopped\n");
    }

    private Response readyStatus() {
        var status = node.status();
        if (draining) {
            return Response.unavailable("not ready: shutting down\n");
        }
        if (!status.running()) {
            return Response.unavailable("not ready: node stopped\n");
        }
        return switch (status.role()) {
            case LEADER -> Response.ok();
            case FOLLOWER -> status.leader().isPresent()
                ? Response.ok()
                : Response.unavailable("not ready: leader unknown\n");
            case PRE_CANDIDATE, CANDIDATE -> Response.unavailable("not ready: role=" + status.role() + "\n");
        };
    }

    private static void write(HttpExchange exchange, Response response) throws IOException {
        byte[] bytes = response.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", response.contentType());
        exchange.sendResponseHeaders(response.code(), bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private record Response(int code, String contentType, String body) {
        private static Response ok() {
            return new Response(200, TEXT, "ok\n");
        }

        private static Response unavailable(String body) {
            return new Response(503, TEXT, body);
        }
    }
}
