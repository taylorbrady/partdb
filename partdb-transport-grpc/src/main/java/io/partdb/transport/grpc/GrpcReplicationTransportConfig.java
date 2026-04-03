package io.partdb.transport.grpc;

import java.util.Map;

record GrpcReplicationTransportConfig(
    String localNodeId,
    int port,
    Map<String, PeerEndpoint> raftPeerEndpoints,
    int snapshotChunkSize
) {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    public GrpcReplicationTransportConfig {
        if (localNodeId == null || localNodeId.isEmpty()) {
            throw new IllegalArgumentException("localNodeId is required");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive");
        }
        raftPeerEndpoints = Map.copyOf(raftPeerEndpoints);
        if (snapshotChunkSize <= 0) {
            snapshotChunkSize = DEFAULT_CHUNK_SIZE;
        }
    }

    public static GrpcReplicationTransportConfig create(
            String localNodeId,
            int port,
            Map<String, String> raftPeerAddresses) {
        return new GrpcReplicationTransportConfig(
            localNodeId,
            port,
            raftPeerAddresses.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    entry -> PeerEndpoint.parse(entry.getValue())
                )),
            DEFAULT_CHUNK_SIZE
        );
    }
}
