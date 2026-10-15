package io.partdb.server;

import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.ClusterId;
import io.partdb.raft.runtime.SnapshotPolicy;
import io.partdb.server.peer.PeerEndpoint;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerConfigTest {

    private static ServerConfig create(String nodeId, Map<String, String> peers, int clientPort, int adminPort) {
        return ServerConfig.create(nodeId, peers, Path.of("data", nodeId), clientPort, adminPort, Optional.empty(), SnapshotPolicy.DEFAULT);
    }

    @Test
    void derivesVotersAndTheRaftPortFromPeers() {
        var config = create("node2", Map.of("node1", "127.0.0.1:8100", "node2", "[::1]:8110"), 8111, 8112);

        assertEquals(Set.of(NodeId.of("node1"), NodeId.of("node2")), config.node().raft().initialMembership().members());
        assertEquals(Map.of(
            NodeId.of("node1"), new PeerEndpoint("127.0.0.1", 8100),
            NodeId.of("node2"), new PeerEndpoint("::1", 8110)
        ), config.peers());
        assertEquals(Optional.of(8110), config.raftPort());
    }

    @Test
    void derivesTheSameClusterIdOnEveryNode() {
        var peers = Map.of("node1", "127.0.0.1:8100", "node2", "127.0.0.1:8200");
        var first = create("node1", peers, 8101, 8102);
        var second = create("node2", peers, 8201, 8202);

        assertEquals(first.node().raft().clusterId(), second.node().raft().clusterId());
    }

    @Test
    void usesAnExplicitClusterId() {
        var config = ServerConfig.create("node1", Map.of(), Path.of("data/node1"), 8101, 8102, Optional.of(ClusterId.of("prod")), SnapshotPolicy.DEFAULT);

        assertEquals(ClusterId.of("prod"), config.node().raft().clusterId());
    }

    @Test
    void singleNodeNeedsNoRaftPort() {
        var config = create("node1", Map.of(), 8101, 8102);

        assertEquals(Optional.empty(), config.raftPort());
    }

    @Test
    void rejectsPeersThatExcludeTheLocalNode() {
        assertThrows(IllegalArgumentException.class, () -> create("node2", Map.of("node1", "127.0.0.1:8100"), 8101, 8102));
    }

    @Test
    void rejectsCollidingPorts() {
        assertThrows(IllegalArgumentException.class, () -> create("node1", Map.of(), 8100, 8100));
        assertThrows(IllegalArgumentException.class, () -> create("node1", Map.of("node1", "127.0.0.1:8100"), 8100, 8102));
    }

    @Test
    void rejectsInvalidPeerAddresses() {
        assertThrows(IllegalArgumentException.class, () -> create("node1", Map.of("node1", "127.0.0.1"), 8101, 8102));
    }
}
