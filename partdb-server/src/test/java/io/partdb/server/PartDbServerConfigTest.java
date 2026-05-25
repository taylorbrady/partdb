package io.partdb.server;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartDbServerConfigTest {

    @Test
    void createKeepsRaftPeerAddressesInTransportLayer() {
        var config = PartDbServerConfig.create(
            "node2",
            Map.of(
                "node1", "127.0.0.1:8100",
                "node2", "127.0.0.1:8101"
            ),
            Path.of("data/node2"),
            8100,
            8101,
            8102
        );

        assertEquals(Set.of("node1", "node2"), config.nodeConfig().memberIds());
        assertEquals(
            Map.of(
                "node1", "127.0.0.1:8100",
                "node2", "127.0.0.1:8101"
            ),
            config.raftPeerAddresses()
        );
    }

    @Test
    void createRejectsRaftPeerAddressesThatExcludeLocalNode() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PartDbServerConfig.create(
                "node2",
                Map.of("node1", "127.0.0.1:8100"),
                Path.of("data/node2"),
                8100,
                8101,
                8102
            )
        );
    }

    @Test
    void createCanonicalizesIpv6RaftPeerAddresses() {
        var config = PartDbServerConfig.create(
            "node1",
            Map.of("node1", "[::1]:8100"),
            Path.of("data/node1"),
            8100,
            8101,
            8102
        );

        assertEquals("[::1]:8100", config.raftPeerAddresses().get("node1"));
    }

    @Test
    void createRejectsCollidingPorts() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PartDbServerConfig.create(
                "node1",
                Map.of(),
                Path.of("data/node1"),
                8100,
                8100,
                8102
            )
        );
    }

    @Test
    void createRejectsInvalidRaftPeerAddresses() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PartDbServerConfig.create(
                "node1",
                Map.of("node1", "127.0.0.1"),
                Path.of("data/node1"),
                8100,
                8101,
                8102
            )
        );
    }
}
