package io.partdb.node;

import io.partdb.consensus.ClusterMembership;
import io.partdb.consensus.ConsensusConfig;
import io.partdb.storage.StorageOptions;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class PartDbNodeConfig {
    private final Path dataDirectory;
    private final StorageOptions storageOptions;
    private final ConsensusConfig consensusConfig;

    private PartDbNodeConfig(Builder builder) {
        this.dataDirectory = Objects.requireNonNull(builder.dataDirectory, "dataDirectory must not be null");
        this.storageOptions = Objects.requireNonNull(builder.storageOptions, "storageOptions must not be null");
        this.consensusConfig = builder.consensusConfigBuilder.build();
    }

    public String nodeId() {
        return consensusConfig.nodeId();
    }

    public ClusterMembership membership() {
        return consensusConfig.membership();
    }

    public Set<String> memberIds() {
        return consensusConfig.membership().memberIds();
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public ConsensusConfig consensusConfig() {
        return consensusConfig;
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(nodeId, dataDirectory);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartDbNodeConfig other)) {
            return false;
        }
        return consensusConfig.equals(other.consensusConfig)
            && dataDirectory.equals(other.dataDirectory)
            && storageOptions.equals(other.storageOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId(), consensusConfig, dataDirectory, storageOptions);
    }

    @Override
    public String toString() {
        return "PartDbNodeConfig{"
            + "nodeId='" + nodeId() + '\''
            + ", membership=" + membership()
            + ", dataDirectory=" + dataDirectory
            + ", storageOptions=" + storageOptions
            + ", consensusConfig=" + consensusConfig
            + '}';
    }

    public static final class Builder {
        private final String nodeId;
        private final Path dataDirectory;
        private StorageOptions storageOptions = StorageOptions.defaults();
        private final ConsensusConfig.Builder consensusConfigBuilder;
        private ClusterMembership membership;

        private Builder(String nodeId, Path dataDirectory) {
            this.nodeId = nodeId;
            this.dataDirectory = dataDirectory;
            this.consensusConfigBuilder = ConsensusConfig.builder(nodeId);
            this.membership = ClusterMembership.ofVoters(nodeId);
        }

        public Builder voters(String... voterIds) {
            membership = new ClusterMembership(toIdSet(voterIds, "voterIds"), membership.learners());
            return this;
        }

        public Builder learners(String... learnerIds) {
            membership = new ClusterMembership(membership.voters(), toIdSet(learnerIds, "learnerIds"));
            return this;
        }

        public Builder membership(ClusterMembership membership) {
            this.membership = Objects.requireNonNull(membership, "membership must not be null");
            return this;
        }

        public Builder storageOptions(StorageOptions storageOptions) {
            this.storageOptions = Objects.requireNonNull(storageOptions, "storageOptions must not be null");
            return this;
        }

        public Builder tickInterval(java.time.Duration tickInterval) {
            consensusConfigBuilder.tickInterval(tickInterval);
            return this;
        }

        public Builder electionTimeoutMinTicks(int ticks) {
            consensusConfigBuilder.electionTimeoutMinTicks(ticks);
            return this;
        }

        public Builder electionTimeoutMaxTicks(int ticks) {
            consensusConfigBuilder.electionTimeoutMaxTicks(ticks);
            return this;
        }

        public Builder heartbeatIntervalTicks(int ticks) {
            consensusConfigBuilder.heartbeatIntervalTicks(ticks);
            return this;
        }

        public Builder maxEntriesPerAppend(int count) {
            consensusConfigBuilder.maxEntriesPerAppend(count);
            return this;
        }

        public PartDbNodeConfig build() {
            consensusConfigBuilder.membership(membership);
            return new PartDbNodeConfig(this);
        }
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static Set<String> toIdSet(String[] ids, String name) {
        Objects.requireNonNull(ids, name + " must not be null");
        var normalized = new LinkedHashSet<String>();
        for (String id : ids) {
            normalized.add(requireNonBlank(id, "memberId"));
        }
        return Set.copyOf(normalized);
    }
}
