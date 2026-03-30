package io.partdb.node;

import io.partdb.raft.Membership;
import io.partdb.raft.RaftConfig;
import io.partdb.storage.StorageConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class PartDbNodeConfig {
    private static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    private final String nodeId;
    private final Membership membership;
    private final Path dataDirectory;
    private final StorageConfig storageConfig;
    private final RaftConfig raftConfig;
    private final Duration tickInterval;

    private PartDbNodeConfig(Builder builder) {
        this.nodeId = requireNonBlank(builder.nodeId, "nodeId");
        this.dataDirectory = Objects.requireNonNull(builder.dataDirectory, "dataDirectory must not be null");
        this.storageConfig = Objects.requireNonNull(builder.storageConfig, "storageConfig must not be null");
        this.raftConfig = Objects.requireNonNull(builder.raftConfig, "raftConfig must not be null");
        this.tickInterval = Objects.requireNonNull(builder.tickInterval, "tickInterval must not be null");
        if (tickInterval.isNegative() || tickInterval.isZero()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }
        this.membership = builder.membership != null
            ? Objects.requireNonNull(builder.membership, "membership must not be null")
            : Membership.ofVoters(nodeId);
        validateMembership(this.membership);
        if (!membership.isMember(nodeId)) {
            throw new IllegalArgumentException("membership must include nodeId");
        }
    }

    public String nodeId() {
        return nodeId;
    }

    Membership membership() {
        return membership;
    }

    public Set<String> memberIds() {
        var memberIds = new LinkedHashSet<String>();
        memberIds.addAll(membership.voters());
        memberIds.addAll(membership.learners());
        return Set.copyOf(memberIds);
    }

    Path dataDirectory() {
        return dataDirectory;
    }

    StorageConfig storageConfig() {
        return storageConfig;
    }

    RaftConfig raftConfig() {
        return raftConfig;
    }

    Duration tickInterval() {
        return tickInterval;
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
        return nodeId.equals(other.nodeId)
            && membership.equals(other.membership)
            && dataDirectory.equals(other.dataDirectory)
            && storageConfig.equals(other.storageConfig)
            && raftConfig.equals(other.raftConfig)
            && tickInterval.equals(other.tickInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, membership, dataDirectory, storageConfig, raftConfig, tickInterval);
    }

    @Override
    public String toString() {
        return "PartDbNodeConfig{"
            + "nodeId='" + nodeId + '\''
            + ", membership=" + membership
            + ", dataDirectory=" + dataDirectory
            + ", storageConfig=" + storageConfig
            + ", raftConfig=" + raftConfig
            + ", tickInterval=" + tickInterval
            + '}';
    }

    public static final class Builder {
        private final String nodeId;
        private final Path dataDirectory;
        private Membership membership;
        private StorageConfig storageConfig = StorageConfig.defaults();
        private RaftConfig raftConfig = RaftConfig.defaults();
        private Duration tickInterval = DEFAULT_TICK_INTERVAL;

        private Builder(String nodeId, Path dataDirectory) {
            this.nodeId = nodeId;
            this.dataDirectory = dataDirectory;
        }

        public Builder members(String... memberIds) {
            Objects.requireNonNull(memberIds, "memberIds must not be null");
            this.membership = Membership.ofVoters(memberIds);
            return this;
        }

        public Builder membership(Membership membership) {
            this.membership = Objects.requireNonNull(membership, "membership must not be null");
            return this;
        }

        public Builder storageConfig(StorageConfig storageConfig) {
            this.storageConfig = Objects.requireNonNull(storageConfig, "storageConfig must not be null");
            return this;
        }

        public Builder raftConfig(RaftConfig raftConfig) {
            this.raftConfig = Objects.requireNonNull(raftConfig, "raftConfig must not be null");
            return this;
        }

        public Builder tickInterval(Duration tickInterval) {
            this.tickInterval = Objects.requireNonNull(tickInterval, "tickInterval must not be null");
            return this;
        }

        public PartDbNodeConfig build() {
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

    private static void validateMembership(Membership membership) {
        membership.voters().forEach(voterId -> requireNonBlank(voterId, "voterId"));
        membership.learners().forEach(learnerId -> requireNonBlank(learnerId, "learnerId"));
    }
}
