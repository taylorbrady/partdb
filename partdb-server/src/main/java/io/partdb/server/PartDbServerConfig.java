package io.partdb.server;

import io.partdb.node.PartDbNodeConfig;
import io.partdb.server.grpc.KvServerConfig;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

public record PartDbServerConfig(
    PartDbNodeConfig nodeConfig,
    int raftPort,
    KvServerConfig kvServerConfig
) {
    public PartDbServerConfig {
        Objects.requireNonNull(nodeConfig, "nodeConfig must not be null");
        Objects.requireNonNull(kvServerConfig, "kvServerConfig must not be null");
    }

    public String nodeId() {
        return nodeConfig.nodeId();
    }

    public Map<String, String> peerAddresses() {
        return nodeConfig.peerAddresses();
    }

    public static PartDbServerConfig create(
        String nodeId,
        Map<String, String> peerAddresses,
        Path dataDirectory,
        int raftPort,
        int kvPort
    ) {
        return new PartDbServerConfig(
            PartDbNodeConfig.create(nodeId, peerAddresses, dataDirectory),
            raftPort,
            KvServerConfig.defaultConfig(kvPort)
        );
    }
}
