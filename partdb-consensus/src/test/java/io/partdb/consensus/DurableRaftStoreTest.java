package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void appendReplacesConflictingDurableSuffix() {
        var membership = RaftMembership.voters("n1", "n2", "n3");

        try (var store = DurableRaftStore.create(tempDir, membership)) {
            store.append(null, List.of(
                data(1, 1, "a"),
                data(2, 1, "old-b"),
                data(3, 1, "old-c")
            ));

            store.append(null, List.of(
                data(2, 2, "new-b"),
                data(3, 2, "new-c")
            ));

            assertEquals(3, store.lastIndex());
            assertEquals(1, store.term(1));
            assertEquals(2, store.term(2));
            assertEquals(2, store.term(3));
            assertEquals(List.of(data(1, 1, "a"), data(2, 2, "new-b"), data(3, 2, "new-c")),
                store.entries(1, 4, Long.MAX_VALUE));
        }
    }

    @Test
    void openRecoversReplacedSuffixWithoutStaleEntries() {
        var membership = RaftMembership.voters("n1", "n2", "n3");

        try (var store = DurableRaftStore.create(tempDir, membership)) {
            store.append(new RaftHardState(1, "n1", 1), List.of(
                data(1, 1, "a"),
                data(2, 1, "old-b"),
                data(3, 1, "old-c")
            ));
            store.append(new RaftHardState(2, "n2", 1), List.of(
                data(2, 2, "new-b")
            ));
            store.sync();
        }

        try (var reopened = DurableRaftStore.open(tempDir)) {
            assertEquals(2, reopened.lastIndex());
            assertEquals(0, reopened.term(3));
            assertEquals(List.of(data(1, 1, "a"), data(2, 2, "new-b")),
                reopened.entries(1, 4, Long.MAX_VALUE));
            assertEquals(new RaftHardState(2, "n2", 1), reopened.bootstrap().hardState().orElseThrow());
        }
    }

    @Test
    void openRecoversHardStateWithoutEntries() {
        var membership = RaftMembership.voters("n1");
        var hardState = new RaftHardState(7, "n1", 0);

        try (var store = DurableRaftStore.create(tempDir, membership)) {
            store.append(hardState, List.of());
            store.sync();
        }

        try (var reopened = DurableRaftStore.open(tempDir)) {
            assertEquals(hardState, reopened.bootstrap().hardState().orElseThrow());
            assertEquals(0, reopened.lastIndex());
        }
    }

    @Test
    void openRecoversSnapshotCompactedPrefix() {
        var membership = RaftMembership.voters("n1", "n2");
        var snapshotMembership = new RaftMembership(Set.of("n1", "n2"), Set.of("n3"));
        var snapshot = new RaftSnapshot(2, 1, snapshotMembership, Bytes.utf8("snapshot"));

        try (var store = DurableRaftStore.create(tempDir, membership)) {
            store.append(new RaftHardState(1, "n1", 2), List.of(
                data(1, 1, "a"),
                data(2, 1, "b"),
                data(3, 1, "c")
            ));
            store.saveSnapshot(snapshot);
            store.sync();
        }

        try (var reopened = DurableRaftStore.open(tempDir)) {
            assertEquals(3, reopened.firstIndex());
            assertEquals(3, reopened.lastIndex());
            assertEquals(1, reopened.term(2));
            assertEquals(List.of(data(3, 1, "c")), reopened.entries(3, 4, Long.MAX_VALUE));
            assertEquals(snapshot, reopened.snapshot().orElseThrow());
            assertEquals(snapshotMembership, reopened.bootstrap().membership().orElseThrow());
            assertThrows(ConsensusException.Compaction.class, () -> reopened.entries(2, 3, Long.MAX_VALUE));
        }
    }

    private static RaftLogEntry.Data data(long index, long term, String value) {
        return new RaftLogEntry.Data(index, term, Bytes.utf8(value));
    }
}
