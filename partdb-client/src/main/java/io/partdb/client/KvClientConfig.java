package io.partdb.client;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public record KvClientConfig(
    List<ServerEndpoint> endpoints,
    Duration requestTimeout,
    Duration connectTimeout,
    int maxRetries,
    Duration retryDelay
) {
    public KvClientConfig {
        Objects.requireNonNull(endpoints, "endpoints must not be null");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connectTimeout must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
        Objects.requireNonNull(retryDelay, "retryDelay must not be null");
        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay must not be negative");
        }
        endpoints = List.copyOf(endpoints);
    }

    public static KvClientConfig defaultConfig(String... endpoints) {
        return defaultConfig(
            List.of(endpoints).stream()
                .map(ServerEndpoint::parse)
                .toArray(ServerEndpoint[]::new)
        );
    }

    public static KvClientConfig defaultConfig(ServerEndpoint... endpoints) {
        return new KvClientConfig(
            List.of(endpoints),
            Duration.ofSeconds(30),
            Duration.ofSeconds(5),
            3,
            Duration.ofMillis(100)
        );
    }
}
