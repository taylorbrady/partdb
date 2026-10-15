package io.partdb.raft.runtime;

import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftOptions;

import java.time.Duration;
import java.util.Objects;

public record RaftConfig(String nodeId, RaftMembership initialMembership, Duration tickInterval, RaftOptions options) {
    public static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    public RaftConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(initialMembership, "initialMembership must not be null");
        Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        Objects.requireNonNull(options, "options must not be null");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (!initialMembership.isMember(nodeId)) {
            throw new IllegalArgumentException("initialMembership must include " + nodeId);
        }
        if (!tickInterval.isPositive()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
    }

    public RaftConfig(String nodeId, RaftMembership initialMembership) {
        this(nodeId, initialMembership, DEFAULT_TICK_INTERVAL, RaftOptions.defaults());
    }
}
