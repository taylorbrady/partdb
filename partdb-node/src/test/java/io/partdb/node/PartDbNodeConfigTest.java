package io.partdb.node;

import io.partdb.node.cluster.ClusterMembership;
import io.partdb.node.config.ReplicationConfig;
import io.partdb.node.config.StorageConfig;
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
        assertEquals(StorageConfig.defaults(), config.storage());
        assertEquals(ReplicationConfig.defaults(), config.replication());
    }

    @Test
    void builderSupportsAdvancedOverrides() {
        var storage = StorageConfig.defaults();
        var config = PartDbNodeConfig.builder("node2", Path.of("data/node2"))
            .voters("node1", "node2")
            .tickInterval(Duration.ofMillis(25))
            .storage(storage)
            .electionTimeoutMinTicks(20)
            .electionTimeoutMaxTicks(40)
            .heartbeatIntervalTicks(5)
            .maxEntriesPerAppend(250)
            .build();

        assertEquals(Set.of("node1", "node2"), config.memberIds());
        assertEquals(storage, config.storage());
        assertEquals(Duration.ofMillis(25), config.replication().tickInterval());
        assertEquals(20, config.replication().electionTimeoutMinTicks());
        assertEquals(40, config.replication().electionTimeoutMaxTicks());
        assertEquals(5, config.replication().heartbeatIntervalTicks());
        assertEquals(250, config.replication().maxEntriesPerAppend());
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
