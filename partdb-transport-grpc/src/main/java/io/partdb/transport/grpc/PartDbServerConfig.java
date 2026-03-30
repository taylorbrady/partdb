package io.partdb.transport.grpc;

import io.partdb.node.PartDbNodeConfig;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class PartDbServerConfig {
    private final PartDbNodeConfig nodeConfig;
    private final Map<String, String> raftPeerAddresses;
    private final int raftPort;
    private final GrpcServerConfig grpcServerConfig;

    PartDbServerConfig(
        PartDbNodeConfig nodeConfig,
        Map<String, String> raftPeerAddresses,
        int raftPort,
        GrpcServerConfig grpcServerConfig
    ) {
        Objects.requireNonNull(nodeConfig, "nodeConfig must not be null");
        Objects.requireNonNull(raftPeerAddresses, "raftPeerAddresses must not be null");
        Objects.requireNonNull(grpcServerConfig, "grpcServerConfig must not be null");
        if (raftPort <= 0 || raftPort > 65535) {
            throw new IllegalArgumentException("raftPort must be between 1 and 65535");
        }
        this.nodeConfig = nodeConfig;
        this.raftPeerAddresses = Map.copyOf(raftPeerAddresses);
        this.raftPort = raftPort;
        this.grpcServerConfig = grpcServerConfig;
    }

    PartDbNodeConfig nodeConfig() {
        return nodeConfig;
    }

    public String nodeId() {
        return nodeConfig.nodeId();
    }

    public Map<String, String> raftPeerAddresses() {
        return raftPeerAddresses;
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
        Map<String, String> raftPeerAddresses,
        Path dataDirectory,
        int raftPort,
        int grpcPort
    ) {
        var normalizedRaftPeerAddresses = normalizeRaftPeerAddresses(nodeId, raftPeerAddresses);
        var nodeConfigBuilder = PartDbNodeConfig.builder(nodeId, dataDirectory);
        if (!normalizedRaftPeerAddresses.isEmpty()) {
            nodeConfigBuilder.voters(normalizedRaftPeerAddresses.keySet().toArray(String[]::new));
        }
        return new PartDbServerConfig(
            nodeConfigBuilder.build(),
            normalizedRaftPeerAddresses,
            raftPort,
            GrpcServerConfig.defaultConfig(grpcPort)
        );
    }

    private static Map<String, String> normalizeRaftPeerAddresses(
        String nodeId,
        Map<String, String> raftPeerAddresses
    ) {
        Objects.requireNonNull(raftPeerAddresses, "raftPeerAddresses must not be null");

        var normalized = new LinkedHashMap<String, String>();
        raftPeerAddresses.forEach((peerId, raftAddress) -> normalized.put(
            requireNonBlank(peerId, "peerId"),
            PeerEndpoint.parse(requireNonBlank(raftAddress, "raftPeerAddress")).toString()
        ));

        if (!normalized.isEmpty() && !normalized.containsKey(nodeId)) {
            throw new IllegalArgumentException("raftPeerAddresses must include the local nodeId");
        }
        return normalized;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
