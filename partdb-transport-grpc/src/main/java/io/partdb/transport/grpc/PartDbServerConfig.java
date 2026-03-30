package io.partdb.transport.grpc;

import io.partdb.node.PartDbNodeConfig;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

public final class PartDbServerConfig {
    private final PartDbNodeConfig nodeConfig;
    private final int raftPort;
    private final GrpcServerConfig grpcServerConfig;

    PartDbServerConfig(PartDbNodeConfig nodeConfig, int raftPort, GrpcServerConfig grpcServerConfig) {
        Objects.requireNonNull(nodeConfig, "nodeConfig must not be null");
        Objects.requireNonNull(grpcServerConfig, "grpcServerConfig must not be null");
        if (raftPort <= 0 || raftPort > 65535) {
            throw new IllegalArgumentException("raftPort must be between 1 and 65535");
        }
        this.nodeConfig = nodeConfig;
        this.raftPort = raftPort;
        this.grpcServerConfig = grpcServerConfig;
    }

    PartDbNodeConfig nodeConfig() {
        return nodeConfig;
    }

    public String nodeId() {
        return nodeConfig.nodeId();
    }

    public Map<String, String> peerAddresses() {
        return nodeConfig.peerAddresses();
    }

    public int raftPort() {
        return raftPort;
    }

    public int grpcPort() {
        return grpcServerConfig.port();
    }

    GrpcServerConfig grpcServerConfig() {
        return grpcServerConfig;
    }

    public static PartDbServerConfig create(
        String nodeId,
        Map<String, String> peerAddresses,
        Path dataDirectory,
        int raftPort,
        int grpcPort
    ) {
        return new PartDbServerConfig(
            PartDbNodeConfig.builder(nodeId, dataDirectory)
                .peerAddresses(peerAddresses)
                .build(),
            raftPort,
            GrpcServerConfig.defaultConfig(grpcPort)
        );
    }
}
