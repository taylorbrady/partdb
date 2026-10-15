package io.partdb.node;

import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftOptions;
import io.partdb.raft.runtime.RaftConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

public record PartDbNodeConfig(Path dataDirectory, RaftConfig raft) {
    public PartDbNodeConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(raft, "raft must not be null");
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(nodeId, dataDirectory);
    }

    public String nodeId() {
        return raft.nodeId();
    }

    public PartDbNodeConfig withSnapshotThreshold(long threshold) {
        return new PartDbNodeConfig(dataDirectory, raft.withSnapshotThreshold(threshold));
    }

    public static final class Builder {
        private final String nodeId;
        private final Path dataDirectory;
        private RaftMembership membership;
        private Duration tickInterval = RaftConfig.DEFAULT_TICK_INTERVAL;
        private RaftOptions raftOptions = RaftOptions.defaults();
        private long snapshotThreshold = RaftConfig.DEFAULT_SNAPSHOT_THRESHOLD;

        private Builder(String nodeId, Path dataDirectory) {
            this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
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

        public Builder snapshotThreshold(long snapshotThreshold) {
            this.snapshotThreshold = snapshotThreshold;
            return this;
        }

        public PartDbNodeConfig build() {
            return new PartDbNodeConfig(dataDirectory, new RaftConfig(nodeId, membership, tickInterval, raftOptions, snapshotThreshold));
        }
    }
}
