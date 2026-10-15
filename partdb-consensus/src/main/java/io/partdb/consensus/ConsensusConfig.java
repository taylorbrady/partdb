package io.partdb.consensus;

import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftOptions;

import java.time.Duration;
import java.util.Objects;

public record ConsensusConfig(
    String nodeId,
    RaftMembership membership,
    Duration tickInterval,
    RaftOptions raftOptions
) {
    public static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    public ConsensusConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        membership = Objects.requireNonNull(membership, "membership must not be null");
        tickInterval = Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        raftOptions = Objects.requireNonNull(raftOptions, "raftOptions must not be null");
        if (!membership.isMember(nodeId)) {
            throw new IllegalArgumentException("membership must include nodeId");
        }
        if (tickInterval.isZero() || tickInterval.isNegative()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
    }
}
