package io.partdb.raft;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public record ClusterConfig(String nodeId, List<Peer> peers) {

    public ClusterConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(peers, "peers must not be null");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        for (Peer peer : peers) {
            if (peer.nodeId().equals(nodeId)) {
                throw new IllegalArgumentException(
                    "peers must not include self (nodeId: " + nodeId + ")");
            }
        }
        peers = List.copyOf(peers);
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    public int quorum() {
        return (clusterSize() / 2) + 1;
    }

    public List<String> peerNodeIds() {
        return peers.stream().map(Peer::nodeId).toList();
    }

    public Map<String, String> peerAddressMap() {
        return peers.stream()
            .collect(Collectors.toMap(Peer::nodeId, Peer::address));
    }
}
