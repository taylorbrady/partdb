package io.partdb.server;

import io.partdb.raft.ClusterConfig;
import io.partdb.raft.RaftConfig;
import io.partdb.raft.transport.RaftTransportConfig;
import io.partdb.server.grpc.KvServerConfig;
import io.partdb.storage.StoreConfig;

import java.nio.file.Path;
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
        ClusterConfig cluster,
        Path dataDirectory,
        int raftPort,
        int kvPort
    ) {
        return new PartDbConfig(
            dataDirectory,
            StoreConfig.create(),
            RaftConfig.create(cluster, dataDirectory.resolve("raft")),
            RaftTransportConfig.defaultConfig("0.0.0.0", raftPort, cluster.peers()),
            KvServerConfig.defaultConfig(kvPort)
        );
    }
}
