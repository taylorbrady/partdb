package io.partdb.server;

import io.partdb.raft.RaftConfig;
import io.partdb.raft.transport.RaftTransportConfig;
import io.partdb.server.grpc.KvServerConfig;
import io.partdb.storage.StoreConfig;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record PartDbConfig(
    Path dataDirectory,
    StoreConfig storeConfig,
    RaftConfig raftConfig,
    RaftTransportConfig raftTransportConfig,
    KvServerConfig kvServerConfig
) {
    public PartDbConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(storeConfig, "storeConfig must not be null");
        Objects.requireNonNull(raftConfig, "raftConfig must not be null");
        Objects.requireNonNull(raftTransportConfig, "raftTransportConfig must not be null");
        Objects.requireNonNull(kvServerConfig, "kvServerConfig must not be null");
    }

    public static PartDbConfig create(
        String nodeId,
        List<String> peers,
        Map<String, String> peerAddresses,
        Path dataDirectory,
        int raftPort,
        int kvPort
    ) {
        return new PartDbConfig(
            dataDirectory,
            StoreConfig.create(),
            RaftConfig.create(nodeId, peers, dataDirectory.resolve("raft")),
            RaftTransportConfig.defaultConfig("0.0.0.0", raftPort, peerAddresses),
            KvServerConfig.defaultConfig(kvPort)
        );
    }
}
