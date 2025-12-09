package io.partdb.server;

import io.partdb.raft.RaftConfig;
import io.partdb.server.grpc.KvServerConfig;
import io.partdb.storage.LSMConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public record PartDbServerConfig(
    String nodeId,
    List<String> peers,
    Path dataDirectory,
    LSMConfig storeConfig,
    RaftConfig raftConfig,
    Duration tickInterval,
    int raftPort,
    KvServerConfig kvServerConfig
) {
    public PartDbServerConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(peers, "peers must not be null");
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(storeConfig, "storeConfig must not be null");
        Objects.requireNonNull(raftConfig, "raftConfig must not be null");
        Objects.requireNonNull(tickInterval, "tickInterval must not be null");
        Objects.requireNonNull(kvServerConfig, "kvServerConfig must not be null");
    }

    public static PartDbServerConfig create(
        String nodeId,
        List<String> peerSpecs,
        Path dataDirectory,
        int raftPort,
        int kvPort
    ) {
        return new PartDbServerConfig(
            nodeId,
            peerSpecs,
            dataDirectory,
            LSMConfig.defaults(),
            RaftConfig.defaults(),
            Duration.ofMillis(10),
            raftPort,
            KvServerConfig.defaultConfig(kvPort)
        );
    }
}
