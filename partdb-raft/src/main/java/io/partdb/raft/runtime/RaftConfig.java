package io.partdb.raft.runtime;

import io.partdb.raft.NodeId;
import io.partdb.raft.Options;

import java.time.Duration;
import java.util.Objects;

public record RaftConfig(
    NodeId nodeId,
    Bootstrap bootstrap,
    Duration tickInterval,
    Options options,
    LogRetention retention
) {
    public static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    public RaftConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(bootstrap, "bootstrap must not be null");
        Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        Objects.requireNonNull(options, "options must not be null");
        Objects.requireNonNull(retention, "retention must not be null");
        if (!bootstrap.membership().isMember(nodeId)) {
            throw new IllegalArgumentException("the bootstrap membership must include " + nodeId);
        }
        if (!tickInterval.isPositive()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
    }
}
