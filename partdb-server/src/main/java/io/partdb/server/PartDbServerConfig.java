package io.partdb.server;

import io.partdb.raft.RaftConfig;
import io.partdb.server.grpc.KvServerConfig;
import io.partdb.storage.LSMConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public record PartDbServerConfig(
    String nodeId,
    Map<String, String> peerAddresses,
    Path dataDirectory,
    LSMConfig storeConfig,
    RaftConfig raftConfig,
    Duration tickInterval,
    int raftPort,
    KvServerConfig kvServerConfig
) {
    public PartDbServerConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(peerAddresses, "peerAddresses must not be null");
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(storeConfig, "storeConfig must not be null");
        Objects.requireNonNull(raftConfig, "raftConfig must not be null");
        Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        Objects.requireNonNull(kvServerConfig, "kvServerConfig must not be null");
        peerAddresses = Map.copyOf(peerAddresses);
    }

    public Set<String> peerIds() {
        return peerAddresses.keySet();
    }

    public static PartDbServerConfig create(
        String nodeId,
        Map<String, String> peerAddresses,
        Path dataDirectory,
        int raftPort,
        int kvPort
    ) {
        return new PartDbServerConfig(
            nodeId,
            peerAddresses,
            dataDirectory,
            LSMConfig.defaults(),
            RaftConfig.defaults(),
            Duration.ofMillis(10),
            raftPort,
            KvServerConfig.defaultConfig(kvPort)
        );
    }
}
