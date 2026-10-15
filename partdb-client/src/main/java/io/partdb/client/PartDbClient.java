package io.partdb.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class PartDbClient implements AutoCloseable {

    private final Connections connections;
    private final KvClient kv;
    private final ClusterClient cluster;

    private PartDbClient(Builder builder) {
        this.connections = new Connections(builder.endpoints, builder.connectTimeout);
        var invoker = new Invoker(connections, builder.requestTimeout, builder.retry);
        this.kv = new KvClient(invoker);
        this.cluster = new ClusterClient(connections, invoker, builder.requestTimeout);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static PartDbClient connect(String... endpoints) {
        return builder().endpoints(endpoints).build();
    }

    public KvClient kv() {
        return kv;
    }

    public ClusterClient cluster() {
        return cluster;
    }

    public List<ServerEndpoint> endpoints() {
        return connections.endpoints();
    }

    @Override
    public void close() {
        connections.close();
    }

    public static final class Builder {
        private final List<ServerEndpoint> endpoints = new ArrayList<>();
        private Duration requestTimeout = Duration.ofSeconds(10);
        private Duration connectTimeout = Duration.ofSeconds(3);
        private RetryPolicy retry = RetryPolicy.DEFAULT;

        private Builder() {
        }

        public Builder endpoints(String... endpoints) {
            return endpoints(Arrays.stream(endpoints).map(ServerEndpoint::parse).toList());
        }

        public Builder endpoints(List<ServerEndpoint> endpoints) {
            Objects.requireNonNull(endpoints, "endpoints must not be null");
            for (ServerEndpoint endpoint : endpoints) {
                if (!this.endpoints.contains(Objects.requireNonNull(endpoint, "endpoint must not be null"))) {
                    this.endpoints.add(endpoint);
                }
            }
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = positive(requestTimeout, "requestTimeout");
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = positive(connectTimeout, "connectTimeout");
            return this;
        }

        public Builder retry(RetryPolicy retry) {
            this.retry = Objects.requireNonNull(retry, "retry must not be null");
            return this;
        }

        public PartDbClient build() {
            if (endpoints.isEmpty()) {
                throw new IllegalArgumentException("at least one endpoint is required");
            }
            return new PartDbClient(this);
        }

        private static Duration positive(Duration value, String name) {
            Objects.requireNonNull(value, name + " must not be null");
            if (!value.isPositive()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        }
    }
}
