package io.partdb.node;

import io.partdb.raft.RaftConfig;
import io.partdb.storage.LSMConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbNodeConfigTest {

    @Test
    void builderUsesProductDefaults() {
        var config = PartDbNodeConfig.builder("node1", Path.of("data/node1"))
            .build();

        assertEquals("node1", config.nodeId());
        assertEquals(Path.of("data/node1"), config.dataDirectory());
        assertTrue(config.peerAddresses().isEmpty());
        assertEquals(LSMConfig.defaults(), config.storeConfig());
        assertEquals(RaftConfig.defaults(), config.raftConfig());
        assertEquals(Duration.ofMillis(10), config.tickInterval());
    }

    @Test
    void builderSupportsAdvancedOverrides() {
        var storeConfig = LSMConfig.defaults();
        var raftConfig = new RaftConfig(20, 40, 5, 250);
        var config = PartDbNodeConfig.builder("node2", Path.of("data/node2"))
            .peer("node1", "127.0.0.1:8100")
            .peer("node2", "127.0.0.1:8101")
            .tickInterval(Duration.ofMillis(25))
            .storageConfig(storeConfig)
            .raftConfig(raftConfig)
            .build();

        assertEquals(
            Map.of(
                "node1", "127.0.0.1:8100",
                "node2", "127.0.0.1:8101"
            ),
            config.peerAddresses()
        );
        assertEquals(storeConfig, config.storeConfig());
        assertEquals(raftConfig, config.raftConfig());
        assertEquals(Duration.ofMillis(25), config.tickInterval());
    }

    @Test
    void peerAddressesAreImmutable() {
        var config = PartDbNodeConfig.builder("node1", Path.of("data/node1"))
            .peer("node1", "127.0.0.1:8100")
            .build();

        assertThrows(
            UnsupportedOperationException.class,
            () -> config.peerAddresses().put("node2", "127.0.0.1:8101")
        );
    }
}
