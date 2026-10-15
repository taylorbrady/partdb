package io.partdb.client.testing;

import io.partdb.client.ServerEndpoint;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record NodeSpec(
    String nodeId,
    int raftPort,
    int grpcPort,
    int adminPort,
    Map<String, String> raftPeerAddresses
) {
    public NodeSpec {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        raftPeerAddresses = Collections.unmodifiableMap(new LinkedHashMap<>(raftPeerAddresses));
    }

    public ServerEndpoint grpcEndpoint() {
        return new ServerEndpoint("localhost", grpcPort);
    }
}
