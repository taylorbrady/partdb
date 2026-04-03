package io.partdb.consensus;

import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftMembership;
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
    void openRecoversMembershipFromCommittedConfigEntriesOnly() {
        var initialMembership = RaftMembership.ofVoters("n1");
        var committedMembership = RaftMembership.ofVoters("n1", "n2");
        var uncommittedMembership = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));

        try (var store = DurableRaftStore.create(tempDir, initialMembership)) {
            store.append(
                new RaftPersistentState(1, "n1", 1),
                List.of(
                    new LogEntry.Config(1, 1, committedMembership),
                    new LogEntry.Config(2, 1, uncommittedMembership)
                )
            );
            store.sync();
        }

        try (var reopened = DurableRaftStore.open(tempDir)) {
            assertEquals(committedMembership, reopened.bootstrap().membership().orElseThrow());
        }
    }
}
