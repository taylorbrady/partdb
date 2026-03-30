package io.partdb.transport.grpc;

import io.partdb.node.PartDbNodeConfig;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class PartDbServerConfig {
    private final PartDbNodeConfig nodeConfig;
    private final Map<String, String> peerAddresses;
    private final int raftPort;
    private final GrpcServerConfig grpcServerConfig;

    PartDbServerConfig(
        PartDbNodeConfig nodeConfig,
        Map<String, String> peerAddresses,
        int raftPort,
        GrpcServerConfig grpcServerConfig
    ) {
        Objects.requireNonNull(nodeConfig, "nodeConfig must not be null");
        Objects.requireNonNull(peerAddresses, "peerAddresses must not be null");
        Objects.requireNonNull(grpcServerConfig, "grpcServerConfig must not be null");
        if (raftPort <= 0 || raftPort > 65535) {
            throw new IllegalArgumentException("raftPort must be between 1 and 65535");
        }
        this.nodeConfig = nodeConfig;
        this.peerAddresses = Map.copyOf(peerAddresses);
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
        return peerAddresses;
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
        var normalizedPeerAddresses = normalizePeerAddresses(nodeId, peerAddresses);
        var nodeConfigBuilder = PartDbNodeConfig.builder(nodeId, dataDirectory);
        if (!normalizedPeerAddresses.isEmpty()) {
            nodeConfigBuilder.members(normalizedPeerAddresses.keySet().toArray(String[]::new));
        }
        return new PartDbServerConfig(
            nodeConfigBuilder.build(),
            normalizedPeerAddresses,
            raftPort,
            GrpcServerConfig.defaultConfig(grpcPort)
        );
    }

    private static Map<String, String> normalizePeerAddresses(String nodeId, Map<String, String> peerAddresses) {
        Objects.requireNonNull(peerAddresses, "peerAddresses must not be null");

        var normalized = new LinkedHashMap<String, String>();
        peerAddresses.forEach((peerId, address) -> normalized.put(
            requireNonBlank(peerId, "peerId"),
            requireNonBlank(address, "peerAddress")
        ));

        if (!normalized.isEmpty() && !normalized.containsKey(nodeId)) {
            throw new IllegalArgumentException("peerAddresses must include the local nodeId");
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
