package io.partdb.server;

import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.ClusterId;
import io.partdb.server.peer.PeerEndpoint;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerConfigTest {

    private static ServerConfig.Builder builder(String nodeId) {
        return ServerConfig.builder(nodeId, Path.of("data", nodeId));
    }

    @Test
    void derivesVotersAndTheRaftPortFromPeers() {
        var config = builder("node2").peer("node1", "127.0.0.1:8100").peer("node2", "[::1]:8110").clientPort(8111).adminPort(8112).build();

        assertEquals(Set.of(NodeId.of("node1"), NodeId.of("node2")), config.node().raft().bootstrap().membership().members());
        assertEquals(Map.of(
            NodeId.of("node1"), new PeerEndpoint("127.0.0.1", 8100),
            NodeId.of("node2"), new PeerEndpoint("::1", 8110)
        ), config.peers());
        assertEquals(Optional.of(8110), config.raftPort());
    }

    @Test
    void derivesTheSameClusterIdOnEveryNode() {
        var first = builder("node1").peer("node1", "127.0.0.1:8100").peer("node2", "127.0.0.1:8200").build();
        var second = builder("node2").peer("node2", "127.0.0.1:8200").peer("node1", "127.0.0.1:8100").clientPort(8201).adminPort(8202).build();

        assertEquals(first.clusterId(), second.clusterId());
    }

    @Test
    void derivesDifferentClusterIdsForClustersOnDifferentAddresses() {
        var first = builder("node1").peer("node1", "10.0.0.1:8100").peer("node2", "10.0.0.2:8100").build();
        var second = builder("node1").peer("node1", "10.0.1.1:8100").peer("node2", "10.0.1.2:8100").build();

        assertNotEquals(first.clusterId(), second.clusterId());
    }

    @Test
    void usesAnExplicitClusterId() {
        assertEquals(ClusterId.of("prod"), builder("node1").clusterId("prod").build().clusterId());
    }

    @Test
    void defaultsToSingleNodeOnTheStandardPorts() {
        var config = builder("node1").build();

        assertEquals(Optional.empty(), config.raftPort());
        assertEquals(ServerConfig.DEFAULT_CLIENT_PORT, config.clientPort());
        assertEquals(ServerConfig.DEFAULT_ADMIN_PORT, config.adminPort());
        assertEquals(Set.of(NodeId.of("node1")), config.node().raft().bootstrap().membership().members());
    }

    @Test
    void rejectsPeersThatExcludeTheLocalNode() {
        assertThrows(IllegalArgumentException.class, () -> builder("node2").peer("node1", "127.0.0.1:8100").build());
    }

    @Test
    void rejectsDuplicatePeers() {
        assertThrows(IllegalArgumentException.class, () -> builder("node1").peer("node1", "127.0.0.1:8100").peer("node1", "127.0.0.1:8200"));
    }

    @Test
    void rejectsCollidingPorts() {
        assertThrows(IllegalArgumentException.class, () -> builder("node1").clientPort(8100).adminPort(8100).build());
        assertThrows(IllegalArgumentException.class, () -> builder("node1").peer("node1", "127.0.0.1:8100").clientPort(8100).build());
    }

    @Test
    void rejectsInvalidPeerAddresses() {
        assertThrows(IllegalArgumentException.class, () -> builder("node1").peer("node1", "127.0.0.1"));
    }
}
