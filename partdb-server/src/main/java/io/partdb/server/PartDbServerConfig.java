package io.partdb.server;

import io.partdb.node.PartDbNodeConfig;
import io.partdb.server.peer.PeerEndpoint;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record PartDbServerConfig(
    PartDbNodeConfig nodeConfig,
    Map<String, PeerEndpoint> raftPeerEndpoints,
    int raftPort,
    int grpcPort,
    int adminPort
) {
    public PartDbServerConfig {
        Objects.requireNonNull(nodeConfig, "nodeConfig must not be null");
        raftPeerEndpoints = Map.copyOf(Objects.requireNonNull(raftPeerEndpoints, "raftPeerEndpoints must not be null"));
        validatePort(raftPort, "raftPort");
        validatePort(grpcPort, "grpcPort");
        validatePort(adminPort, "adminPort");
        if (raftPort == grpcPort || raftPort == adminPort || grpcPort == adminPort) {
            throw new IllegalArgumentException("raftPort, grpcPort, and adminPort must be distinct");
        }
    }

    public String nodeId() {
        return nodeConfig.nodeId();
    }

    public static PartDbServerConfig create(
        String nodeId,
        Map<String, String> raftPeerAddresses,
        Path dataDirectory,
        int raftPort,
        int grpcPort,
        int adminPort
    ) {
        Objects.requireNonNull(raftPeerAddresses, "raftPeerAddresses must not be null");
        var endpoints = new LinkedHashMap<String, PeerEndpoint>();
        raftPeerAddresses.forEach((peerId, address) -> endpoints.put(peerId, PeerEndpoint.parse(address)));
        if (!endpoints.isEmpty() && !endpoints.containsKey(nodeId)) {
            throw new IllegalArgumentException("raftPeerAddresses must include the local nodeId");
        }

        var nodeConfig = PartDbNodeConfig.builder(nodeId, dataDirectory);
        if (!endpoints.isEmpty()) {
            nodeConfig.voters(endpoints.keySet().toArray(String[]::new));
        }
        return new PartDbServerConfig(nodeConfig.build(), endpoints, raftPort, grpcPort, adminPort);
    }

    private static void validatePort(int value, String name) {
        if (value <= 0 || value > 65535) {
            throw new IllegalArgumentException(name + " must be between 1 and 65535");
        }
    }
}
