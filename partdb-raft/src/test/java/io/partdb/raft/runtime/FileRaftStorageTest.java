package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.SnapshotMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileRaftStorageTest {

    @TempDir
    Path directory;

    private static RaftLogEntry command(long index, long term, String value) {
        return new RaftLogEntry.Command(index, term, Bytes.utf8(value));
    }

    private static Optional<RaftHardState> state(long term, long commit) {
        return Optional.of(new RaftHardState(term, Optional.of("n1"), commit));
    }

    private Path payload(FileRaftStorage storage, String contents) throws IOException {
        Path staged = storage.stagingFile();
        Files.writeString(staged, contents);
        return staged;
    }

    @Test
    void servesAppendedEntriesAndReplacesConflicts() {
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(state(1, 0), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.append(state(2, 1), List.of(command(2, 2, "x")));

            assertEquals(1, storage.firstIndex());
            assertEquals(2, storage.lastIndex());
            assertEquals(2, storage.term(2));
            assertEquals(List.of(command(1, 1, "a"), command(2, 2, "x")), storage.entries(1, 10, Long.MAX_VALUE));
            assertEquals(List.of(command(1, 1, "a")), storage.entries(1, 10, 0));
        }
    }

    @Test
    void recoversLogAndHardState() {
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(state(1, 0), List.of(command(1, 1, "a"), command(2, 1, "b")));
            storage.append(state(2, 1), List.of(command(2, 2, "x")));
            storage.sync();
        }

        try (var storage = FileRaftStorage.open(directory)) {
            assertEquals(state(2, 1).orElseThrow(), storage.hardState());
            assertEquals(List.of(command(1, 1, "a"), command(2, 2, "x")), storage.entries(1, 10, Long.MAX_VALUE));
        }
    }

    @Test
    void rejectsAppendsThatLeaveGaps() {
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(Optional.empty(), List.of(command(1, 1, "a")));

            assertThrows(IllegalArgumentException.class, () -> storage.append(Optional.empty(), List.of(command(3, 1, "c"))));
        }
    }

    @Test
    void installSnapshotReplacesTheLog() throws IOException {
        var membership = RaftMembership.of(Set.of("n1", "n2"), Set.of("n3"));
        var snapshot = new SnapshotMetadata(5, 2, membership);
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(state(2, 1), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.installSnapshot(snapshot, payload(storage, "state"));
            storage.append(Optional.empty(), List.of(command(6, 2, "f")));
            storage.sync();
        }

        try (var storage = FileRaftStorage.open(directory)) {
            assertEquals(6, storage.firstIndex());
            assertEquals(6, storage.lastIndex());
            assertEquals(2, storage.term(5));
            assertEquals(List.of(command(6, 2, "f")), storage.entries(6, 7, Long.MAX_VALUE));
            var installed = storage.snapshot().orElseThrow();
            assertEquals(snapshot, installed.metadata());
            assertEquals("state", Files.readString(installed.payload()));
            assertEquals(state(2, 1).orElseThrow(), storage.hardState());
            assertThrows(IllegalArgumentException.class, () -> storage.entries(5, 6, Long.MAX_VALUE));
        }
    }

    @Test
    void openDiscardsLogThatDoesNotContinueTheSnapshot() throws IOException {
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(state(1, 1), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.sync();
        }
        installSnapshotOnly(new SnapshotMetadata(2, 2, RaftMembership.voters("n1", "n2")));

        try (var storage = FileRaftStorage.open(directory)) {
            assertEquals(state(1, 1).orElseThrow(), storage.hardState());
            assertEquals(3, storage.firstIndex());
            assertEquals(2, storage.lastIndex());
        }
        try (var storage = FileRaftStorage.open(directory)) {
            assertEquals(2, storage.lastIndex());
        }
    }

    @Test
    void openKeepsLogThatContinuesTheSnapshot() throws IOException {
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(state(1, 2), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.sync();
        }
        installSnapshotOnly(new SnapshotMetadata(2, 1, RaftMembership.voters("n1", "n2")));

        try (var storage = FileRaftStorage.open(directory)) {
            assertEquals(3, storage.firstIndex());
            assertEquals(List.of(command(3, 1, "c")), storage.entries(3, 4, Long.MAX_VALUE));
        }
    }

    @Test
    void compactionDropsEntriesThroughTheSnapshot() throws IOException {
        var snapshot = new SnapshotMetadata(2, 1, RaftMembership.voters("n1"));
        try (var storage = FileRaftStorage.open(directory)) {
            storage.append(state(1, 3), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.compact(snapshot, payload(storage, "state"));

            assertEquals(3, storage.firstIndex());
            assertEquals(1, storage.term(2));
            assertEquals(List.of(command(3, 1, "c")), storage.entries(3, 4, Long.MAX_VALUE));
            assertThrows(IllegalArgumentException.class,
                () -> storage.compact(new SnapshotMetadata(3, 2, RaftMembership.voters("n1")), payload(storage, "x")));
        }

        try (var storage = FileRaftStorage.open(directory)) {
            assertEquals(snapshot, storage.snapshot().orElseThrow().metadata());
            assertEquals(List.of(command(3, 1, "c")), storage.entries(3, 4, Long.MAX_VALUE));
        }
    }

    private void installSnapshotOnly(SnapshotMetadata metadata) throws IOException {
        var snapshots = SnapshotStore.open(directory.resolve("snap"));
        Path staged = snapshots.stagingFile();
        Files.writeString(staged, "payload");
        snapshots.install(metadata, staged);
    }
}
