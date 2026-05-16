package io.partdb.node;

import io.partdb.cluster.ClusterMembership;
import io.partdb.consensus.ConsensusConfig;
import io.partdb.node.config.ReplicationConfig;
import io.partdb.node.config.StorageConfig;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class PartDbNodeConfig {
    private final String nodeId;
    private final Path dataDirectory;
    private final ClusterMembership membership;
    private final StorageConfig storage;
    private final ReplicationConfig replication;

    private PartDbNodeConfig(Builder builder) {
        this.nodeId = requireNonBlank(builder.nodeId, "nodeId");
        this.dataDirectory = Objects.requireNonNull(builder.dataDirectory, "dataDirectory must not be null");
        this.membership = Objects.requireNonNull(builder.membership, "membership must not be null");
        if (!membership.isMember(nodeId)) {
            throw new IllegalArgumentException("membership must include nodeId");
        }
        this.storage = Objects.requireNonNull(builder.storage, "storage must not be null");
        this.replication = Objects.requireNonNull(builder.replication, "replication must not be null");
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(nodeId, dataDirectory);
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public String nodeId() {
        return nodeId;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public ClusterMembership membership() {
        return membership;
    }

    public Set<String> memberIds() {
        return membership.memberIds();
    }

    public StorageConfig storage() {
        return storage;
    }

    public ReplicationConfig replication() {
        return replication;
    }

    public ConsensusConfig toConsensusConfig() {
        return replication.toConsensusConfig(nodeId, membership);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartDbNodeConfig other)) {
            return false;
        }
        return nodeId.equals(other.nodeId)
            && dataDirectory.equals(other.dataDirectory)
            && membership.equals(other.membership)
            && storage.equals(other.storage)
            && replication.equals(other.replication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, dataDirectory, membership, storage, replication);
    }

    @Override
    public String toString() {
        return "PartDbNodeConfig{"
            + "nodeId='" + nodeId + '\''
            + ", dataDirectory=" + dataDirectory
            + ", membership=" + membership
            + ", storage=" + storage
            + ", replication=" + replication
            + '}';
    }

    public static final class Builder {
        private final String nodeId;
        private final Path dataDirectory;
        private ClusterMembership membership;
        private StorageConfig storage = StorageConfig.defaults();
        private ReplicationConfig replication = ReplicationConfig.defaults();
        private ReplicationConfig.Builder replicationBuilder = ReplicationConfig.defaults().toBuilder();

        private Builder(String nodeId, Path dataDirectory) {
            this.nodeId = requireNonBlank(nodeId, "nodeId");
            this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
            this.membership = ClusterMembership.ofVoters(nodeId);
        }

        private Builder(PartDbNodeConfig config) {
            this.nodeId = config.nodeId;
            this.dataDirectory = config.dataDirectory;
            this.membership = config.membership;
            this.storage = config.storage;
            this.replication = config.replication;
            this.replicationBuilder = config.replication.toBuilder();
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

        public Builder storage(StorageConfig storage) {
            this.storage = Objects.requireNonNull(storage, "storage must not be null");
            return this;
        }

        public Builder replication(ReplicationConfig replication) {
            this.replicationBuilder = Objects.requireNonNull(replication, "replication must not be null").toBuilder();
            return this;
        }

        public Builder tickInterval(java.time.Duration tickInterval) {
            this.replicationBuilder.tickInterval(tickInterval);
            return this;
        }

        public Builder electionTimeoutMinTicks(int ticks) {
            this.replicationBuilder.electionTimeoutMinTicks(ticks);
            return this;
        }

        public Builder electionTimeoutMaxTicks(int ticks) {
            this.replicationBuilder.electionTimeoutMaxTicks(ticks);
            return this;
        }

        public Builder heartbeatIntervalTicks(int ticks) {
            this.replicationBuilder.heartbeatIntervalTicks(ticks);
            return this;
        }

        public Builder maxEntriesPerAppend(int count) {
            this.replicationBuilder.maxEntriesPerAppend(count);
            return this;
        }

        public PartDbNodeConfig build() {
            this.replication = replicationBuilder.build();
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
