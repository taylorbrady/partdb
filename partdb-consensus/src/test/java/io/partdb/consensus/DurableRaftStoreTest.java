package io.partdb.consensus;

import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftHardState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurableRaftStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void openRecoversConfigurationFromCommittedConfigEntriesOnly() {
        var initialConfiguration = RaftMembership.voters("n1");
        var committedConfiguration = RaftMembership.voters("n1", "n2");
        var uncommittedConfiguration = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));

        try (var store = DurableRaftStore.create(tempDir, initialConfiguration)) {
            store.append(
                new RaftHardState(1, "n1", 1),
                List.of(
                    new RaftLogEntry.Config(1, 1, committedConfiguration),
                    new RaftLogEntry.Config(2, 1, uncommittedConfiguration)
                )
            );
            store.sync();
        }

        try (var reopened = DurableRaftStore.open(tempDir)) {
            assertEquals(committedConfiguration, reopened.bootstrap().membership().orElseThrow());
        }
    }
}
