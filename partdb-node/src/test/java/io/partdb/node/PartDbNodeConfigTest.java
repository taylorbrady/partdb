package io.partdb.node;

import io.partdb.raft.MembershipChange;
import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.Options;
import io.partdb.raft.runtime.RaftConfig;
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

        assertEquals(Membership.ofVoters(Set.of(NodeId.of("node1"))), config.raft().initialMembership());
        assertEquals(Options.defaults(), config.raft().options());
        assertEquals(RaftConfig.DEFAULT_TICK_INTERVAL, config.raft().tickInterval());
        assertEquals(NodeId.of("node1"), config.nodeId());
    }

    @Test
    void builderSupportsOverrides() {
        var options = new Options(20, 5, 1 << 20, 64, 1 << 26);
        var config = PartDbNodeConfig.builder("node2", Path.of("data/node2"))
            .voters("node1", "node2")
            .tickInterval(Duration.ofMillis(25))
            .raftOptions(options)
            .build();

        assertEquals(Set.of(NodeId.of("node1"), NodeId.of("node2")), config.raft().initialMembership().members());
        assertEquals(Duration.ofMillis(25), config.raft().tickInterval());
        assertEquals(options, config.raft().options());
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
            .membership(Membership.ofVoters(Set.of(NodeId.of("node1"), NodeId.of("node2"))).apply(new MembershipChange.AddLearner(NodeId.of("node3"))))
            .build();

        assertEquals(Set.of(NodeId.of("node1"), NodeId.of("node2"), NodeId.of("node3")), config.raft().initialMembership().members());
    }
}
