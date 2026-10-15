package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteAheadLogTest {

    @TempDir
    Path directory;

    private static RaftLogEntry command(long index, long term, String value) {
        return new RaftLogEntry.Command(index, term, Bytes.utf8(value));
    }

    private static RaftHardState state(long term, long commit) {
        return new RaftHardState(term, Optional.of("n1"), commit);
    }

    private WriteAheadLog open(long segmentBytes) {
        return WriteAheadLog.open(directory, segmentBytes);
    }

    private long segmentCount() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.toString().endsWith(".wal")).count();
        }
    }

    @Test
    void replaysEntriesAndLatestHardState() {
        try (var wal = open(1 << 20)) {
            wal.append(Optional.of(state(1, 0)), List.of(command(1, 1, "a"), command(2, 1, "b")));
            wal.append(Optional.of(state(1, 2)), List.of(new RaftLogEntry.NoOp(3, 1)));
        }

        try (var wal = open(1 << 20)) {
            var replay = wal.replay();
            assertEquals(state(1, 2), replay.hardState());
            assertEquals(List.of(command(1, 1, "a"), command(2, 1, "b"), new RaftLogEntry.NoOp(3, 1)), replay.entries());
            assertEquals(OptionalLong.empty(), replay.resetIndex());
        }
    }

    @Test
    void replayTruncatesAtOverwrittenEntries() {
        try (var wal = open(1 << 20)) {
            wal.append(Optional.empty(), List.of(command(1, 1, "a"), command(2, 1, "b"), command(3, 1, "c")));
            wal.append(Optional.empty(), List.of(command(2, 2, "x")));
        }

        try (var wal = open(1 << 20)) {
            assertEquals(List.of(command(1, 1, "a"), command(2, 2, "x")), wal.replay().entries());
        }
    }

    @Test
    void roundTripsEveryEntryType() {
        var membership = RaftMembership.of(Set.of("n1", "n2"), Set.of("n3"));
        var entries = List.of(command(1, 1, "a"), new RaftLogEntry.NoOp(2, 1), new RaftLogEntry.Membership(3, 1, membership));
        try (var wal = open(1 << 20)) {
            wal.append(Optional.of(new RaftHardState(1, Optional.empty(), 0)), entries);
        }

        try (var wal = open(1 << 20)) {
            assertEquals(entries, wal.replay().entries());
            assertEquals(new RaftHardState(1, Optional.empty(), 0), wal.replay().hardState());
        }
    }

    @Test
    void resetStartsAfterSnapshotAndKeepsHardState() throws IOException {
        try (var wal = open(1 << 20)) {
            wal.append(Optional.of(state(3, 2)), LongStream.rangeClosed(1, 10).mapToObj(i -> command(i, 1, "v" + i)).toList());
            wal.reset(20, 2);
            wal.append(Optional.empty(), List.of(command(21, 3, "u")));
        }

        try (var wal = open(1 << 20)) {
            var replay = wal.replay();
            assertEquals(OptionalLong.of(20), replay.resetIndex());
            assertEquals(List.of(command(21, 3, "u")), replay.entries());
            assertEquals(state(3, 2), replay.hardState());
        }
        assertEquals(1, segmentCount());
    }

    @Test
    void recoveryDiscardsSegmentsBeforeAnInterruptedReset() throws IOException {
        Path stale = Files.createDirectory(directory.resolveSibling(directory.getFileName() + "-stale"));
        try (var wal = open(1 << 20)) {
            wal.append(Optional.of(state(1, 0)), List.of(command(1, 1, "a")));
            wal.sync();
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : files.toList()) {
                    Files.copy(file, stale.resolve(file.getFileName()));
                }
            }
            wal.reset(9, 2);
        }
        try (Stream<Path> files = Files.list(stale)) {
            for (Path file : files.toList()) {
                Files.move(file, directory.resolve(file.getFileName()));
            }
        }

        try (var wal = open(1 << 20)) {
            assertEquals(OptionalLong.of(9), wal.replay().resetIndex());
            assertEquals(List.of(), wal.replay().entries());
        }
        assertEquals(1, segmentCount());
    }

    @Test
    void repairsTornTail() throws IOException {
        try (var wal = open(1 << 20)) {
            wal.append(Optional.empty(), List.of(command(1, 1, "a"), command(2, 1, "b")));
        }
        Path segment;
        try (Stream<Path> files = Files.list(directory)) {
            segment = files.findFirst().orElseThrow();
        }
        try (var channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 3);
        }

        try (var wal = open(1 << 20)) {
            assertEquals(List.of(command(1, 1, "a")), wal.replay().entries());
            wal.append(Optional.empty(), List.of(command(2, 2, "c")));
        }
        try (var wal = open(1 << 20)) {
            assertEquals(List.of(command(1, 1, "a"), command(2, 2, "c")), wal.replay().entries());
        }
    }

    @Test
    void rejectsCorruptionBeforeTheLastSegment() throws IOException {
        try (var wal = open(64)) {
            wal.append(Optional.empty(), LongStream.rangeClosed(1, 10).mapToObj(i -> command(i, 1, "value-" + i)).toList());
        }
        Path first;
        try (Stream<Path> files = Files.list(directory)) {
            first = files.sorted().findFirst().orElseThrow();
        }
        try (var channel = FileChannel.open(first, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 1);
        }

        assertThrows(RaftStorageException.Corruption.class, () -> open(64));
    }

    @Test
    void compactionDeletesSegmentsBelowSnapshotButKeepsHardState() throws IOException {
        try (var wal = open(64)) {
            for (long i = 1; i <= 10; i++) {
                wal.append(Optional.of(state(1, i)), List.of(command(i, 1, "value-" + i)));
            }
            long before = segmentCount();
            wal.compact(6);
            assertTrue(segmentCount() < before);
        }

        try (var wal = open(64)) {
            var replay = wal.replay();
            assertEquals(state(1, 10), replay.hardState());
            assertEquals(10, replay.entries().getLast().index());
            assertTrue(replay.entries().getFirst().index() <= 7);
        }
    }
}
