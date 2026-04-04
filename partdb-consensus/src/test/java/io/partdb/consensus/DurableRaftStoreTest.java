package io.partdb.consensus;

import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftConfiguration;
import io.partdb.raft.RaftPersistentState;
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
        var initialConfiguration = RaftConfiguration.ofVoters("n1");
        var committedConfiguration = RaftConfiguration.ofVoters("n1", "n2");
        var uncommittedConfiguration = new RaftConfiguration(Set.of("n1", "n2"), Set.of("n3"));

        try (var store = DurableRaftStore.create(tempDir, initialConfiguration)) {
            store.append(
                new RaftPersistentState(1, "n1", 1),
                List.of(
                    new LogEntry.Config(1, 1, committedConfiguration),
                    new LogEntry.Config(2, 1, uncommittedConfiguration)
                )
            );
            store.sync();
        }

        try (var reopened = DurableRaftStore.open(tempDir)) {
            assertEquals(committedConfiguration, reopened.bootstrap().configuration().orElseThrow());
        }
    }
}
