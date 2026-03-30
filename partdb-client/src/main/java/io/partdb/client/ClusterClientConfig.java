package io.partdb.client;

import java.time.Duration;
import java.util.Objects;

public record ClusterClientConfig(
    ServerEndpoint endpoint,
    Duration requestTimeout,
    Duration connectTimeout
) {
    public ClusterClientConfig {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connectTimeout must be positive");
        }
    }

    public static ClusterClientConfig defaultConfig(String endpoint) {
        return defaultConfig(ServerEndpoint.parse(endpoint));
    }

    public static ClusterClientConfig defaultConfig(ServerEndpoint endpoint) {
        return new ClusterClientConfig(
            endpoint,
            Duration.ofSeconds(30),
            Duration.ofSeconds(5)
        );
    }
}
