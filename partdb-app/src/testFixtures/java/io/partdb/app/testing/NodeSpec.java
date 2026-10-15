package io.partdb.app.testing;

import io.partdb.client.ServerEndpoint;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record NodeSpec(String nodeId, int clientPort, int adminPort, Map<String, String> peers) {
    public NodeSpec {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        peers = Collections.unmodifiableMap(new LinkedHashMap<>(peers));
    }

    public ServerEndpoint endpoint() {
        return new ServerEndpoint("localhost", clientPort);
    }
}
