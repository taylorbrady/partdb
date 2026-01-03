package io.partdb.server.raft;

import java.util.Map;

public record GrpcRaftTransportConfig(
    String localNodeId,
    int port,
    Map<String, String> peerAddresses,
    int snapshotChunkSize
) {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    public GrpcRaftTransportConfig {
        if (localNodeId == null || localNodeId.isEmpty()) {
            throw new IllegalArgumentException("localNodeId is required");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive");
        }
        peerAddresses = Map.copyOf(peerAddresses);
        if (snapshotChunkSize <= 0) {
            snapshotChunkSize = DEFAULT_CHUNK_SIZE;
        }
    }

    public static GrpcRaftTransportConfig create(
            String localNodeId,
            int port,
            Map<String, String> peerAddresses) {
        return new GrpcRaftTransportConfig(localNodeId, port, peerAddresses, DEFAULT_CHUNK_SIZE);
    }
}
