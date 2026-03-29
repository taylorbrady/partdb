package io.partdb.transport.grpc;

import java.time.Duration;
import java.util.Objects;

public record GrpcServerConfig(
    int port,
    Duration defaultTimeout,
    Duration shutdownGracePeriod
) {
    public GrpcServerConfig {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        Objects.requireNonNull(defaultTimeout, "defaultTimeout must not be null");
        Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod must not be null");
        if (defaultTimeout.isNegative() || defaultTimeout.isZero()) {
            throw new IllegalArgumentException("defaultTimeout must be positive");
        }
        if (shutdownGracePeriod.isNegative()) {
            throw new IllegalArgumentException("shutdownGracePeriod must not be negative");
        }
    }

    public static GrpcServerConfig defaultConfig(int port) {
        return new GrpcServerConfig(
            port,
            Duration.ofSeconds(30),
            Duration.ofSeconds(5)
        );
    }
}
