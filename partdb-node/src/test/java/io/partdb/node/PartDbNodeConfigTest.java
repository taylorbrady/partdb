package io.partdb.node;

import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartDbNodeConfigTest {

    @Test
    void builderDefaultsToSingleVoterWithProductDefaults() {
        var config = PartDbNodeConfig.builder("node1", Path.of("data/node1")).build();

        assertEquals(RaftMembership.voters("node1"), config.membership());
        assertEquals(Set.of("node1"), config.memberIds());
        assertEquals(RaftOptions.defaults(), config.raftOptions());
        assertEquals("node1", config.toConsensusConfig().nodeId());
    }

    @Test
    void builderSupportsOverrides() {
        var options = new RaftOptions(20, 40, 5, 250);
        var config = PartDbNodeConfig.builder("node2", Path.of("data/node2"))
            .voters("node1", "node2")
            .tickInterval(Duration.ofMillis(25))
            .raftOptions(options)
            .build();

        assertEquals(Set.of("node1", "node2"), config.memberIds());
        assertEquals(Duration.ofMillis(25), config.tickInterval());
        assertEquals(options, config.toConsensusConfig().raftOptions());
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
    void builderSupportsLearnerMembership() {
        var config = PartDbNodeConfig.builder("node3", Path.of("data/node3"))
            .membership(RaftMembership.voters("node1", "node2").addLearner("node3"))
            .build();

        assertEquals(Set.of("node1", "node2", "node3"), config.memberIds());
    }
}
