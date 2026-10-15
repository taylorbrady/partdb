package io.partdb.server;

import io.partdb.node.NodeConfig;
import io.partdb.node.kv.KvLimits;
import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.Options;
import io.partdb.raft.runtime.ClusterId;
import io.partdb.raft.runtime.RaftConfig;
import io.partdb.raft.runtime.SnapshotPolicy;
import io.partdb.server.peer.PeerEndpoint;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public record ServerConfig(NodeConfig node, Map<NodeId, PeerEndpoint> peers, int clientPort, int adminPort) {
    public ServerConfig {
        Objects.requireNonNull(node, "node must not be null");
        peers = Map.copyOf(Objects.requireNonNull(peers, "peers must not be null"));
        validatePort(clientPort, "clientPort");
        validatePort(adminPort, "adminPort");
        if (clientPort == adminPort) {
            throw new IllegalArgumentException("clientPort and adminPort must differ");
        }
        var self = node.raft().nodeId();
        if (!peers.isEmpty()) {
            if (!peers.containsKey(self)) {
                throw new IllegalArgumentException("peers must include the local node " + self);
            }
            if (!peers.keySet().equals(node.raft().initialMembership().members())) {
                throw new IllegalArgumentException("peers must name exactly the initial cluster members");
            }
            int raftPort = peers.get(self).port();
            if (raftPort == clientPort || raftPort == adminPort) {
                throw new IllegalArgumentException("the raft port of " + self + " must differ from clientPort and adminPort");
            }
        } else if (node.raft().initialMembership().members().size() != 1) {
            throw new IllegalArgumentException("a cluster of several nodes needs peer addresses");
        }
    }

    public static ServerConfig create(
        String nodeId,
        Map<String, String> peerAddresses,
        Path dataDirectory,
        int clientPort,
        int adminPort,
        Optional<ClusterId> clusterId,
        SnapshotPolicy snapshots
    ) {
        Objects.requireNonNull(peerAddresses, "peerAddresses must not be null");
        var self = NodeId.of(nodeId);
        var peers = new LinkedHashMap<NodeId, PeerEndpoint>();
        peerAddresses.forEach((peer, address) -> peers.put(NodeId.of(peer), PeerEndpoint.parse(address)));
        var membership = Membership.ofVoters(peers.isEmpty() ? Set.of(self) : peers.keySet());
        var raft = new RaftConfig(
            clusterId.orElseGet(() -> ClusterId.derivedFrom(membership)),
            self,
            membership,
            RaftConfig.DEFAULT_TICK_INTERVAL,
            Options.defaults(),
            snapshots
        );
        return new ServerConfig(new NodeConfig(dataDirectory, raft, KvLimits.DEFAULT), peers, clientPort, adminPort);
    }

    public NodeId nodeId() {
        return node.raft().nodeId();
    }

    public Optional<Integer> raftPort() {
        return Optional.ofNullable(peers.get(nodeId())).map(PeerEndpoint::port);
    }

    private static void validatePort(int value, String name) {
        if (value <= 0 || value > 65535) {
            throw new IllegalArgumentException(name + " must be between 1 and 65535");
        }
    }
}
