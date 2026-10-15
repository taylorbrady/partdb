package io.partdb.raft.runtime;

import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.Options;

import java.time.Duration;
import java.util.Objects;

public record RaftConfig(
    ClusterId clusterId,
    NodeId nodeId,
    Membership initialMembership,
    Duration tickInterval,
    Options options,
    SnapshotPolicy snapshots
) {
    public static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    public RaftConfig {
        Objects.requireNonNull(clusterId, "clusterId must not be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(initialMembership, "initialMembership must not be null");
        Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        Objects.requireNonNull(options, "options must not be null");
        Objects.requireNonNull(snapshots, "snapshots must not be null");
        if (!initialMembership.isMember(nodeId)) {
            throw new IllegalArgumentException("initialMembership must include " + nodeId);
        }
        if (!tickInterval.isPositive()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
    }

    public static RaftConfig of(NodeId nodeId, Membership initialMembership) {
        return new RaftConfig(
            ClusterId.derivedFrom(initialMembership),
            nodeId,
            initialMembership,
            DEFAULT_TICK_INTERVAL,
            Options.defaults(),
            SnapshotPolicy.DEFAULT
        );
    }
}
