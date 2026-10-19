package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Ids;
import io.partdb.raft.SnapshotMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LogStoreTest {

    @TempDir
    Path directory;

    private static LogEntry command(long index, long term, String value) {
        return new LogEntry.Command(index, term, Bytes.utf8(value));
    }

    private static Optional<HardState> state(long term, long commit) {
        return Optional.of(new HardState(term, Optional.of(Ids.id("n1")), commit));
    }

    @Test
    void servesAppendedEntriesAndReplacesConflicts() {
        try (var storage = LogStore.open(directory)) {
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
        try (var storage = LogStore.open(directory)) {
            storage.append(state(1, 0), List.of(command(1, 1, "a"), command(2, 1, "b")));
            storage.append(state(2, 1), List.of(command(2, 2, "x")));
            storage.sync();
        }

        try (var storage = LogStore.open(directory)) {
            assertEquals(state(2, 1).orElseThrow(), storage.hardState());
            assertEquals(List.of(command(1, 1, "a"), command(2, 2, "x")), storage.entries(1, 10, Long.MAX_VALUE));
        }
    }

    @Test
    void rejectsAppendsThatLeaveGaps() {
        try (var storage = LogStore.open(directory)) {
            storage.append(Optional.empty(), List.of(command(1, 1, "a")));

            assertThrows(IllegalArgumentException.class, () -> storage.append(Optional.empty(), List.of(command(3, 1, "c"))));
        }
    }

    @Test
    void resetReplacesTheLogWithANewBase() {
        var base = new SnapshotMetadata(5, 2, Ids.membership(Set.of("n1", "n2"), Set.of("n3")));
        try (var storage = LogStore.open(directory)) {
            storage.append(state(2, 1), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.reset(base);
            storage.append(Optional.empty(), List.of(command(6, 2, "f")));
            storage.sync();
        }

        try (var storage = LogStore.open(directory)) {
            assertEquals(Optional.of(base), storage.base());
            assertEquals(6, storage.firstIndex());
            assertEquals(6, storage.lastIndex());
            assertEquals(2, storage.term(5));
            assertEquals(List.of(command(6, 2, "f")), storage.entries(6, 7, Long.MAX_VALUE));
            assertEquals(state(2, 1).orElseThrow(), storage.hardState());
            assertThrows(IllegalArgumentException.class, () -> storage.entries(5, 6, Long.MAX_VALUE));
        }
    }

    @Test
    void compactionDropsEntriesThroughTheBase() {
        var base = new SnapshotMetadata(2, 1, Ids.voters("n1"));
        try (var storage = LogStore.open(directory)) {
            storage.append(state(1, 3), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            storage.compact(base);

            assertEquals(3, storage.firstIndex());
            assertEquals(1, storage.term(2));
            assertEquals(1, storage.retainedEntries());
            assertEquals(List.of(command(3, 1, "c")), storage.entries(3, 4, Long.MAX_VALUE));
            assertThrows(IllegalArgumentException.class, () -> storage.compact(new SnapshotMetadata(3, 2, Ids.voters("n1"))));
            assertThrows(IllegalArgumentException.class, () -> storage.compact(new SnapshotMetadata(4, 1, Ids.voters("n1"))));
        }

        try (var storage = LogStore.open(directory)) {
            assertEquals(Optional.of(base), storage.base());
            assertEquals(List.of(command(3, 1, "c")), storage.entries(3, 4, Long.MAX_VALUE));
        }
    }

    @Test
    void compactionIndexKeepsTheRetainedTailBehindTheDurableIndex() {
        try (var storage = LogStore.open(directory)) {
            storage.append(state(1, 10), LongStream.rangeClosed(1, 10).mapToObj(i -> command(i, 1, "0123456789")).toList());

            assertEquals(5, storage.compactionIndex(8, new LogRetention(3, 1_000)));
            assertEquals(6, storage.compactionIndex(8, new LogRetention(3, 25)));
            assertEquals(0, storage.compactionIndex(2, new LogRetention(3, 1_000)));
            assertEquals(7, storage.compactionIndex(20, new LogRetention(3, 1_000)));
        }
    }

    @Test
    void openRejectsAWalThatDoesNotContinueItsBase() throws IOException {
        try (var wal = WriteAheadLog.open(directory.resolve("wal"), 1 << 20)) {
            wal.reset(new SnapshotMetadata(5, 1, Ids.voters("n1")));
            wal.append(Optional.empty(), List.of(command(7, 1, "gap")));
            wal.sync();
        }

        assertThrows(RaftStorageException.Corruption.class, () -> LogStore.open(directory));
    }
}
