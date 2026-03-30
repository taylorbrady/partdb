package io.partdb.transport.grpc;

import java.util.Map;

record GrpcConsensusTransportConfig(
    String localNodeId,
    int port,
    Map<String, String> raftPeerAddresses,
    int snapshotChunkSize
) {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    public GrpcConsensusTransportConfig {
        if (localNodeId == null || localNodeId.isEmpty()) {
            throw new IllegalArgumentException("localNodeId is required");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive");
        }
        raftPeerAddresses = Map.copyOf(raftPeerAddresses);
        if (snapshotChunkSize <= 0) {
            snapshotChunkSize = DEFAULT_CHUNK_SIZE;
        }
    }

    public static GrpcConsensusTransportConfig create(
            String localNodeId,
            int port,
            Map<String, String> raftPeerAddresses) {
        return new GrpcConsensusTransportConfig(localNodeId, port, raftPeerAddresses, DEFAULT_CHUNK_SIZE);
    }
}
