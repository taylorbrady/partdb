package io.partdb.node;

import io.partdb.raft.RaftConfig;
import io.partdb.storage.LSMConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class PartDbNodeConfig {
    private static final Duration DEFAULT_TICK_INTERVAL = Duration.ofMillis(10);

    private final String nodeId;
    private final Map<String, String> peerAddresses;
    private final Path dataDirectory;
    private final LSMConfig storeConfig;
    private final RaftConfig raftConfig;
    private final Duration tickInterval;

    private PartDbNodeConfig(Builder builder) {
        this.nodeId = requireNonBlank(builder.nodeId, "nodeId");
        this.dataDirectory = Objects.requireNonNull(builder.dataDirectory, "dataDirectory must not be null");
        this.storeConfig = Objects.requireNonNull(builder.storeConfig, "storeConfig must not be null");
        this.raftConfig = Objects.requireNonNull(builder.raftConfig, "raftConfig must not be null");
        this.tickInterval = Objects.requireNonNull(builder.tickInterval, "tickInterval must not be null");
        if (tickInterval.isNegative() || tickInterval.isZero()) {
            throw new IllegalArgumentException("tickInterval must be positive");
        }

        var peerCopy = new LinkedHashMap<String, String>();
        builder.peerAddresses.forEach((peerId, address) ->
            peerCopy.put(
                requireNonBlank(peerId, "peerId"),
                requireNonBlank(address, "peerAddress")
            )
        );
        this.peerAddresses = Collections.unmodifiableMap(peerCopy);
    }

    public String nodeId() {
        return nodeId;
    }

    public Map<String, String> peerAddresses() {
        return peerAddresses;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public LSMConfig storeConfig() {
        return storeConfig;
    }

    public RaftConfig raftConfig() {
        return raftConfig;
    }

    public Duration tickInterval() {
        return tickInterval;
    }

    public Set<String> peerIds() {
        return peerAddresses.keySet();
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(nodeId, dataDirectory);
    }

    public static PartDbNodeConfig create(
        String nodeId,
        Map<String, String> peerAddresses,
        Path dataDirectory
    ) {
        return builder(nodeId, dataDirectory)
            .peerAddresses(peerAddresses)
            .build();
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
            && peerAddresses.equals(other.peerAddresses)
            && dataDirectory.equals(other.dataDirectory)
            && storeConfig.equals(other.storeConfig)
            && raftConfig.equals(other.raftConfig)
            && tickInterval.equals(other.tickInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, peerAddresses, dataDirectory, storeConfig, raftConfig, tickInterval);
    }

    @Override
    public String toString() {
        return "PartDbNodeConfig{"
            + "nodeId='" + nodeId + '\''
            + ", peerAddresses=" + peerAddresses
            + ", dataDirectory=" + dataDirectory
            + ", storeConfig=" + storeConfig
            + ", raftConfig=" + raftConfig
            + ", tickInterval=" + tickInterval
            + '}';
    }

    public static final class Builder {
        private final String nodeId;
        private final Path dataDirectory;
        private final Map<String, String> peerAddresses = new LinkedHashMap<>();
        private LSMConfig storeConfig = LSMConfig.defaults();
        private RaftConfig raftConfig = RaftConfig.defaults();
        private Duration tickInterval = DEFAULT_TICK_INTERVAL;

        private Builder(String nodeId, Path dataDirectory) {
            this.nodeId = nodeId;
            this.dataDirectory = dataDirectory;
        }

        public Builder peer(String peerId, String address) {
            peerAddresses.put(peerId, address);
            return this;
        }

        public Builder peerAddresses(Map<String, String> peerAddresses) {
            Objects.requireNonNull(peerAddresses, "peerAddresses must not be null");
            this.peerAddresses.putAll(peerAddresses);
            return this;
        }

        public Builder storageConfig(LSMConfig storeConfig) {
            this.storeConfig = Objects.requireNonNull(storeConfig, "storeConfig must not be null");
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
}
