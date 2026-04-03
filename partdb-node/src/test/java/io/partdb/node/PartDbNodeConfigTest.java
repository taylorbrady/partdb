package io.partdb.node;

import io.partdb.consensus.ClusterMembership;
import io.partdb.consensus.ConsensusConfig;
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
        assertEquals(ClusterMembership.ofVoters("node1"), config.membership());
        assertEquals(Set.of("node1"), config.memberIds());
        assertEquals(StorageOptions.defaults(), config.storageOptions());
        assertEquals(ConsensusConfig.builder("node1").build(), config.consensusConfig());
    }

    @Test
    void builderSupportsAdvancedOverrides() {
        var storageOptions = StorageOptions.defaults();
        var config = PartDbNodeConfig.builder("node2", Path.of("data/node2"))
            .voters("node1", "node2")
            .tickInterval(Duration.ofMillis(25))
            .storageOptions(storageOptions)
            .electionTimeoutMinTicks(20)
            .electionTimeoutMaxTicks(40)
            .heartbeatIntervalTicks(5)
            .maxEntriesPerAppend(250)
            .build();

        assertEquals(Set.of("node1", "node2"), config.memberIds());
        assertEquals(storageOptions, config.storageOptions());
        assertEquals(Duration.ofMillis(25), config.consensusConfig().tickInterval());
        assertEquals(20, config.consensusConfig().electionTimeoutMinTicks());
        assertEquals(40, config.consensusConfig().electionTimeoutMaxTicks());
        assertEquals(5, config.consensusConfig().heartbeatIntervalTicks());
        assertEquals(250, config.consensusConfig().maxEntriesPerAppend());
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
            .membership(ClusterMembership.ofVoters("node1", "node2").addLearner("node3"))
            .build();

        assertEquals(Set.of("node1", "node2", "node3"), config.memberIds());
    }
}
