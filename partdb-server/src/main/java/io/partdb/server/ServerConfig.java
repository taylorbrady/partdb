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

public record ServerConfig(
    NodeId nodeId,
    Path dataDirectory,
    ClusterId clusterId,
    Map<NodeId, PeerEndpoint> peers,
    int clientPort,
    int adminPort,
    SnapshotPolicy snapshots
) {
    public static final int DEFAULT_CLIENT_PORT = 8101;
    public static final int DEFAULT_ADMIN_PORT = 8102;

    public ServerConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(clusterId, "clusterId must not be null");
        peers = Map.copyOf(Objects.requireNonNull(peers, "peers must not be null"));
        Objects.requireNonNull(snapshots, "snapshots must not be null");
        validatePort(clientPort, "clientPort");
        validatePort(adminPort, "adminPort");
        if (clientPort == adminPort) {
            throw new IllegalArgumentException("clientPort and adminPort must differ");
        }
        if (!peers.isEmpty()) {
            if (!peers.containsKey(nodeId)) {
                throw new IllegalArgumentException("peers must include the local node " + nodeId);
            }
            int raftPort = peers.get(nodeId).port();
            if (raftPort == clientPort || raftPort == adminPort) {
                throw new IllegalArgumentException("the raft port of " + nodeId + " must differ from clientPort and adminPort");
            }
        }
    }

    public static Builder builder(String nodeId, Path dataDirectory) {
        return new Builder(NodeId.of(nodeId), Objects.requireNonNull(dataDirectory, "dataDirectory must not be null"));
    }

    public Optional<Integer> raftPort() {
        return Optional.ofNullable(peers.get(nodeId)).map(PeerEndpoint::port);
    }

    NodeConfig node() {
        var raft = new RaftConfig(clusterId, nodeId, membership(nodeId, peers), RaftConfig.DEFAULT_TICK_INTERVAL, Options.defaults(), snapshots);
        return new NodeConfig(dataDirectory, raft, KvLimits.DEFAULT);
    }

    private static Membership membership(NodeId nodeId, Map<NodeId, PeerEndpoint> peers) {
        return Membership.ofVoters(peers.isEmpty() ? Set.of(nodeId) : peers.keySet());
    }

    private static void validatePort(int value, String name) {
        if (value <= 0 || value > 65535) {
            throw new IllegalArgumentException(name + " must be between 1 and 65535");
        }
    }

    public static final class Builder {
        private final NodeId nodeId;
        private final Path dataDirectory;
        private final Map<NodeId, PeerEndpoint> peers = new LinkedHashMap<>();
        private Optional<ClusterId> clusterId = Optional.empty();
        private int clientPort = DEFAULT_CLIENT_PORT;
        private int adminPort = DEFAULT_ADMIN_PORT;
        private SnapshotPolicy snapshots = SnapshotPolicy.DEFAULT;

        private Builder(NodeId nodeId, Path dataDirectory) {
            this.nodeId = nodeId;
            this.dataDirectory = dataDirectory;
        }

        public Builder peer(String nodeId, String address) {
            var id = NodeId.of(nodeId);
            if (peers.putIfAbsent(id, PeerEndpoint.parse(address)) != null) {
                throw new IllegalArgumentException("peer " + id + " is listed more than once");
            }
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = Optional.of(ClusterId.of(clusterId));
            return this;
        }

        public Builder clientPort(int clientPort) {
            this.clientPort = clientPort;
            return this;
        }

        public Builder adminPort(int adminPort) {
            this.adminPort = adminPort;
            return this;
        }

        public Builder snapshotLogEntries(long entries) {
            this.snapshots = new SnapshotPolicy(entries, snapshots.logBytes());
            return this;
        }

        public Builder snapshotLogBytes(long bytes) {
            this.snapshots = new SnapshotPolicy(snapshots.logEntries(), bytes);
            return this;
        }

        public ServerConfig build() {
            return new ServerConfig(
                nodeId,
                dataDirectory,
                clusterId.orElseGet(() -> ClusterId.derivedFrom(membership(nodeId, peers))),
                peers,
                clientPort,
                adminPort,
                snapshots
            );
        }
    }
}
