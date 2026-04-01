package io.partdb.node;

import io.partdb.raft.RaftConfig;
import io.partdb.storage.StorageOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartDbNodeConfigTest {

    @Test
    void builderUsesProductDefaults() {
        var config = PartDbNodeConfig.builder("node1", Path.of("data/node1"))
            .build();

        assertEquals("node1", config.nodeId());
        assertEquals(Path.of("data/node1"), config.dataDirectory());
        assertEquals(NodeMembership.ofVoters("node1"), config.membership());
        assertEquals(Set.of("node1"), config.memberIds());
        assertEquals(StorageOptions.defaults(), config.storageOptions());
        assertEquals(RaftConfig.defaults(), config.raftConfig());
        assertEquals(Duration.ofMillis(10), config.tickInterval());
    }

    @Test
    void builderSupportsAdvancedOverrides() {
        var storageOptions = StorageOptions.defaults();
        var raftConfig = new RaftConfig(20, 40, 5, 250);
        var config = PartDbNodeConfig.builder("node2", Path.of("data/node2"))
            .voters("node1", "node2")
            .tickInterval(Duration.ofMillis(25))
            .storageOptions(storageOptions)
            .raftConfig(raftConfig)
            .build();

        assertEquals(Set.of("node1", "node2"), config.memberIds());
        assertEquals(storageOptions, config.storageOptions());
        assertEquals(raftConfig, config.raftConfig());
        assertEquals(Duration.ofMillis(25), config.tickInterval());
    }

    @Test
    void builderRejectsMembershipThatExcludesLocalNode() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PartDbNodeConfig.builder("node3", Path.of("data/node3"))
                .voters("node1", "node2")
                .build()
        );
    }

    @Test
    void builderSupportsAdvancedMembershipOverrides() {
        var config = PartDbNodeConfig.builder("node3", Path.of("data/node3"))
            .membership(NodeMembership.ofVoters("node1", "node2").addLearner("node3"))
            .build();

        assertEquals(Set.of("node1", "node2", "node3"), config.memberIds());
    }
}
