package io.partdb.server;

import io.partdb.node.PartDbNodeConfig;
import io.partdb.transport.grpc.PeerEndpoint;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class PartDbServerConfig {
    private final PartDbNodeConfig nodeConfig;
    private final Map<String, String> raftPeerAddresses;
    private final PeerEndpoint selfRaftEndpoint;
    private final int raftPort;
    private final int grpcPort;
    private final int adminPort;

    PartDbServerConfig(
        PartDbNodeConfig nodeConfig,
        Map<String, String> raftPeerAddresses,
        PeerEndpoint selfRaftEndpoint,
        int raftPort,
        int grpcPort,
        int adminPort
    ) {
        Objects.requireNonNull(nodeConfig, "nodeConfig must not be null");
        Objects.requireNonNull(raftPeerAddresses, "raftPeerAddresses must not be null");
        Objects.requireNonNull(selfRaftEndpoint, "selfRaftEndpoint must not be null");
        if (raftPort <= 0 || raftPort > 65535) {
            throw new IllegalArgumentException("raftPort must be between 1 and 65535");
        }
        if (grpcPort <= 0 || grpcPort > 65535) {
            throw new IllegalArgumentException("grpcPort must be between 1 and 65535");
        }
        if (adminPort <= 0 || adminPort > 65535) {
            throw new IllegalArgumentException("adminPort must be between 1 and 65535");
        }
        validateDistinctPorts(raftPort, grpcPort, adminPort);
        this.nodeConfig = nodeConfig;
        this.raftPeerAddresses = Map.copyOf(raftPeerAddresses);
        this.selfRaftEndpoint = selfRaftEndpoint;
        this.raftPort = raftPort;
        this.grpcPort = grpcPort;
        this.adminPort = adminPort;
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

    PeerEndpoint selfRaftEndpoint() {
        return selfRaftEndpoint;
    }

    public int raftPort() {
        return raftPort;
    }

    public int grpcPort() {
        return grpcPort;
    }

    public int adminPort() {
        return adminPort;
    }

    public static PartDbServerConfig create(
        String nodeId,
        Map<String, String> raftPeerAddresses,
        Path dataDirectory,
        int raftPort,
        int grpcPort,
        int adminPort
    ) {
        var normalizedRaftPeerAddresses = normalizeRaftPeerAddresses(nodeId, raftPeerAddresses);
        var nodeConfigBuilder = PartDbNodeConfig.builder(nodeId, dataDirectory);
        if (!normalizedRaftPeerAddresses.isEmpty()) {
            nodeConfigBuilder.voters(normalizedRaftPeerAddresses.keySet().toArray(String[]::new));
        }
        return new PartDbServerConfig(
            nodeConfigBuilder.build(),
            normalizedRaftPeerAddresses,
            resolveSelfRaftEndpoint(nodeId, normalizedRaftPeerAddresses, raftPort),
            raftPort,
            grpcPort,
            adminPort
        );
    }

    private static void validateDistinctPorts(int raftPort, int grpcPort, int adminPort) {
        if (raftPort == grpcPort || raftPort == adminPort || grpcPort == adminPort) {
            throw new IllegalArgumentException("raftPort, grpcPort, and adminPort must be distinct");
        }
    }

    private static PeerEndpoint resolveSelfRaftEndpoint(
        String nodeId,
        Map<String, String> raftPeerAddresses,
        int raftPort
    ) {
        String configuredAddress = raftPeerAddresses.get(nodeId);
        if (configuredAddress != null) {
            return PeerEndpoint.parse(configuredAddress);
        }
        return new PeerEndpoint("localhost", raftPort);
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
