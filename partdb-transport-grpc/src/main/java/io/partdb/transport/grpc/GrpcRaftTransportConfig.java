package io.partdb.transport.grpc;

import java.util.Map;

record GrpcRaftTransportConfig(
    String localNodeId,
    int port,
    Map<String, PeerEndpoint> raftPeerEndpoints
) {
    static final int SNAPSHOT_CHUNK_SIZE = 1024 * 1024;

    GrpcRaftTransportConfig {
        if (localNodeId == null || localNodeId.isEmpty()) {
            throw new IllegalArgumentException("localNodeId is required");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive");
        }
        raftPeerEndpoints = Map.copyOf(raftPeerEndpoints);
    }
}
