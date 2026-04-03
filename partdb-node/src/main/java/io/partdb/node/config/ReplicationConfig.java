package io.partdb.node.config;

import io.partdb.consensus.ConsensusConfig;
import io.partdb.node.cluster.ClusterMembership;

import java.time.Duration;
import java.util.Objects;

public record ReplicationConfig(
    Duration tickInterval,
    int electionTimeoutMinTicks,
    int electionTimeoutMaxTicks,
    int heartbeatIntervalTicks,
    int maxEntriesPerAppend
) {
    public ReplicationConfig {
        Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        if (tickInterval.isZero() || tickInterval.isNegative()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
        new ConsensusConfig(
            "validation-node",
            io.partdb.consensus.ClusterMembership.ofVoters("validation-node"),
            tickInterval,
            electionTimeoutMinTicks,
            electionTimeoutMaxTicks,
            heartbeatIntervalTicks,
            maxEntriesPerAppend
        );
    }

    public static ReplicationConfig defaults() {
        var defaults = ConsensusConfig.builder("defaults").build();
        return new ReplicationConfig(
            defaults.tickInterval(),
            defaults.electionTimeoutMinTicks(),
            defaults.electionTimeoutMaxTicks(),
            defaults.heartbeatIntervalTicks(),
            defaults.maxEntriesPerAppend()
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public ConsensusConfig toConsensusConfig(String nodeId, ClusterMembership membership) {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(membership, "membership must not be null");
        return ConsensusConfig.builder(nodeId)
            .membership(new io.partdb.consensus.ClusterMembership(membership.voters(), membership.learners()))
            .tickInterval(tickInterval)
            .electionTimeoutMinTicks(electionTimeoutMinTicks)
            .electionTimeoutMaxTicks(electionTimeoutMaxTicks)
            .heartbeatIntervalTicks(heartbeatIntervalTicks)
            .maxEntriesPerAppend(maxEntriesPerAppend)
            .build();
    }

    public static final class Builder {
        private Duration tickInterval = ReplicationConfig.defaults().tickInterval();
        private int electionTimeoutMinTicks = ReplicationConfig.defaults().electionTimeoutMinTicks();
        private int electionTimeoutMaxTicks = ReplicationConfig.defaults().electionTimeoutMaxTicks();
        private int heartbeatIntervalTicks = ReplicationConfig.defaults().heartbeatIntervalTicks();
        private int maxEntriesPerAppend = ReplicationConfig.defaults().maxEntriesPerAppend();

        private Builder() {
        }

        private Builder(ReplicationConfig config) {
            this.tickInterval = config.tickInterval;
            this.electionTimeoutMinTicks = config.electionTimeoutMinTicks;
            this.electionTimeoutMaxTicks = config.electionTimeoutMaxTicks;
            this.heartbeatIntervalTicks = config.heartbeatIntervalTicks;
            this.maxEntriesPerAppend = config.maxEntriesPerAppend;
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

        public ReplicationConfig build() {
            return new ReplicationConfig(
                tickInterval,
                electionTimeoutMinTicks,
                electionTimeoutMaxTicks,
                heartbeatIntervalTicks,
                maxEntriesPerAppend
            );
        }
    }
}
