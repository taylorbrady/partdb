package io.partdb.node;

import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.Options;
import io.partdb.raft.runtime.ClusterId;
import io.partdb.raft.runtime.RaftConfig;
import io.partdb.raft.runtime.SnapshotPolicy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public record PartDbNodeConfig(Path dataDirectory, RaftConfig raft) {
    public PartDbNodeConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(raft, "raft must not be null");
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(NodeId.of(nodeId), dataDirectory);
    }

    public NodeId nodeId() {
        return raft.nodeId();
    }

    public PartDbNodeConfig withSnapshotThreshold(long threshold) {
        return new PartDbNodeConfig(dataDirectory, new RaftConfig(
            raft.clusterId(), raft.nodeId(), raft.initialMembership(), raft.tickInterval(), raft.options(), SnapshotPolicy.everyEntries(threshold)));
    }

    public static final class Builder {
        private final NodeId nodeId;
        private final Path dataDirectory;
        private Membership membership;
        private Duration tickInterval = RaftConfig.DEFAULT_TICK_INTERVAL;
        private Options raftOptions = Options.defaults();
        private SnapshotPolicy snapshots = SnapshotPolicy.DEFAULT;

        private Builder(NodeId nodeId, Path dataDirectory) {
            this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
            this.dataDirectory = dataDirectory;
            this.membership = Membership.ofVoters(java.util.Set.of(nodeId));
        }

        public Builder voters(String... voterIds) {
            return membership(Membership.ofVoters(Arrays.stream(voterIds).map(NodeId::of).collect(Collectors.toSet())));
        }

        public Builder membership(Membership membership) {
            this.membership = Objects.requireNonNull(membership, "membership must not be null");
            return this;
        }

        public Builder tickInterval(Duration tickInterval) {
            this.tickInterval = Objects.requireNonNull(tickInterval, "tickInterval must not be null");
            return this;
        }

        public Builder raftOptions(Options raftOptions) {
            this.raftOptions = Objects.requireNonNull(raftOptions, "raftOptions must not be null");
            return this;
        }

        public Builder snapshotThreshold(long snapshotThreshold) {
            this.snapshots = SnapshotPolicy.everyEntries(snapshotThreshold);
            return this;
        }

        public PartDbNodeConfig build() {
            return new PartDbNodeConfig(dataDirectory,
                new RaftConfig(ClusterId.derivedFrom(membership), nodeId, membership, tickInterval, raftOptions, snapshots));
        }
    }
}
