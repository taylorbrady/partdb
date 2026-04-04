package io.partdb.consensus;

import io.partdb.cluster.ClusterMembership;
import io.partdb.raft.RaftConfig;

import java.time.Duration;
import java.util.Objects;

public record ConsensusConfig(
    String nodeId,
    ClusterMembership membership,
    Duration tickInterval,
    int electionTimeoutMinTicks,
    int electionTimeoutMaxTicks,
    int heartbeatIntervalTicks,
    int maxEntriesPerAppend
) {
    private static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    public ConsensusConfig {
        nodeId = requireNonBlank(nodeId, "nodeId");
        membership = Objects.requireNonNull(membership, "membership must not be null");
        tickInterval = Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        if (!membership.isMember(nodeId)) {
            throw new IllegalArgumentException("membership must include nodeId");
        }
        if (tickInterval.isZero() || tickInterval.isNegative()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
        new RaftConfig(
            electionTimeoutMinTicks,
            electionTimeoutMaxTicks,
            heartbeatIntervalTicks,
            maxEntriesPerAppend
        );
    }

    public static Builder builder(String nodeId) {
        return new Builder(nodeId);
    }

    RaftConfig toRaftConfig() {
        return new RaftConfig(
            electionTimeoutMinTicks,
            electionTimeoutMaxTicks,
            heartbeatIntervalTicks,
            maxEntriesPerAppend
        );
    }

    public static final class Builder {
        private final String nodeId;
        private ClusterMembership membership;
        private Duration tickInterval = DEFAULT_TICK_INTERVAL;
        private int electionTimeoutMinTicks = RaftConfig.defaults().electionTimeoutMin();
        private int electionTimeoutMaxTicks = RaftConfig.defaults().electionTimeoutMax();
        private int heartbeatIntervalTicks = RaftConfig.defaults().heartbeatInterval();
        private int maxEntriesPerAppend = RaftConfig.defaults().maxEntriesPerAppend();

        private Builder(String nodeId) {
            this.nodeId = requireNonBlank(nodeId, "nodeId");
            this.membership = ClusterMembership.ofVoters(nodeId);
        }

        public Builder membership(ClusterMembership membership) {
            this.membership = Objects.requireNonNull(membership, "membership must not be null");
            return this;
        }

        public Builder tickInterval(Duration tickInterval) {
            this.tickInterval = Objects.requireNonNull(tickInterval, "tickInterval must not be null");
            return this;
        }

        public Builder electionTimeoutMinTicks(int ticks) {
            this.electionTimeoutMinTicks = ticks;
            return this;
        }

        public Builder electionTimeoutMaxTicks(int ticks) {
            this.electionTimeoutMaxTicks = ticks;
            return this;
        }

        public Builder heartbeatIntervalTicks(int ticks) {
            this.heartbeatIntervalTicks = ticks;
            return this;
        }

        public Builder maxEntriesPerAppend(int count) {
            this.maxEntriesPerAppend = count;
            return this;
        }

        public ConsensusConfig build() {
            return new ConsensusConfig(
                nodeId,
                membership,
                tickInterval,
                electionTimeoutMinTicks,
                electionTimeoutMaxTicks,
                heartbeatIntervalTicks,
                maxEntriesPerAppend
            );
        }
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
