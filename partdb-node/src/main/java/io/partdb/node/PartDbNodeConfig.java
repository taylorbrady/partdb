package io.partdb.node;

import io.partdb.consensus.ConsensusConfig;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftOptions;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

public record PartDbNodeConfig(
    String nodeId,
    Path dataDirectory,
    RaftMembership membership,
    Duration tickInterval,
    RaftOptions raftOptions
) {
    public PartDbNodeConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        // ConsensusConfig validates nodeId, membership, tickInterval, and raftOptions.
        new ConsensusConfig(nodeId, membership, tickInterval, raftOptions);
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(nodeId, dataDirectory);
    }

    public Set<String> memberIds() {
        return membership.memberIds();
    }

    public ConsensusConfig toConsensusConfig() {
        return new ConsensusConfig(nodeId, membership, tickInterval, raftOptions);
    }

    public static final class Builder {
        private final String nodeId;
        private final Path dataDirectory;
        private RaftMembership membership;
        private Duration tickInterval = ConsensusConfig.DEFAULT_TICK_INTERVAL;
        private RaftOptions raftOptions = RaftOptions.defaults();

        private Builder(String nodeId, Path dataDirectory) {
            this.nodeId = nodeId;
            this.dataDirectory = dataDirectory;
            this.membership = RaftMembership.voters(nodeId);
        }

        public Builder voters(String... voterIds) {
            return membership(RaftMembership.voters(voterIds));
        }

        public Builder membership(RaftMembership membership) {
            this.membership = Objects.requireNonNull(membership, "membership must not be null");
            return this;
        }

        public Builder tickInterval(Duration tickInterval) {
            this.tickInterval = Objects.requireNonNull(tickInterval, "tickInterval must not be null");
            return this;
        }

        public Builder raftOptions(RaftOptions raftOptions) {
            this.raftOptions = Objects.requireNonNull(raftOptions, "raftOptions must not be null");
            return this;
        }

        public PartDbNodeConfig build() {
            return new PartDbNodeConfig(nodeId, dataDirectory, membership, tickInterval, raftOptions);
        }
    }
}
