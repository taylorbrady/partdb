package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Ids;
import io.partdb.raft.SnapshotMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteAheadLogTest {

    @TempDir
    Path directory;

    private static LogEntry command(long index, long term, String value) {
        return new LogEntry.Command(index, term, Bytes.utf8(value));
    }

    private static HardState state(long term, long commit) {
        return new HardState(term, Optional.of(Ids.id("n1")), commit);
    }

    private static SnapshotMetadata base(long index, long term) {
        return new SnapshotMetadata(index, term, Ids.membership(Set.of("n1", "n2"), Set.of("n3")));
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
            wal.append(Optional.of(state(1, 2)), List.of(new LogEntry.NoOp(3, 1)));
        }

        try (var wal = open(1 << 20)) {
            var replay = wal.replay();
            assertEquals(state(1, 2), replay.hardState());
            assertEquals(List.of(command(1, 1, "a"), command(2, 1, "b"), new LogEntry.NoOp(3, 1)), replay.entries());
            assertEquals(Optional.empty(), replay.base());
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
        var membership = Ids.membership(Set.of("n1", "n2"), Set.of("n3"));
        var entries = List.of(command(1, 1, "a"), new LogEntry.NoOp(2, 1), new LogEntry.Configuration(3, 1, membership));
        try (var wal = open(1 << 20)) {
            wal.append(Optional.of(new HardState(1, Optional.empty(), 0)), entries);
        }

        try (var wal = open(1 << 20)) {
            assertEquals(entries, wal.replay().entries());
            assertEquals(new HardState(1, Optional.empty(), 0), wal.replay().hardState());
        }
    }

    @Test
    void resetStartsAfterTheBaseAndKeepsHardState() throws IOException {
        try (var wal = open(1 << 20)) {
            wal.append(Optional.of(state(3, 2)), LongStream.rangeClosed(1, 10).mapToObj(i -> command(i, 1, "v" + i)).toList());
            wal.reset(base(20, 2));
            wal.append(Optional.empty(), List.of(command(21, 3, "u")));
        }

        try (var wal = open(1 << 20)) {
            var replay = wal.replay();
            assertEquals(Optional.of(base(20, 2)), replay.base());
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
            wal.reset(base(9, 2));
        }
        try (Stream<Path> files = Files.list(stale)) {
            for (Path file : files.toList()) {
                Files.move(file, directory.resolve(file.getFileName()));
            }
        }

        try (var wal = open(1 << 20)) {
            assertEquals(Optional.of(base(9, 2)), wal.replay().base());
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
    void compactionRecordsTheBaseAndDeletesSegmentsBelowIt() throws IOException {
        try (var wal = open(64)) {
            for (long i = 1; i <= 10; i++) {
                wal.append(Optional.of(state(1, i)), List.of(command(i, 1, "value-" + i)));
            }
            long before = segmentCount();
            wal.compact(base(6, 1));
            assertTrue(segmentCount() < before);
        }

        try (var wal = open(64)) {
            var replay = wal.replay();
            assertEquals(Optional.of(base(6, 1)), replay.base());
            assertEquals(state(1, 10), replay.hardState());
            assertEquals(10, replay.entries().getLast().index());
            assertTrue(replay.entries().getFirst().index() <= 7);
        }
    }

    @Test
    void latestBaseWinsAcrossCompactionsAndResets() {
        try (var wal = open(64)) {
            wal.append(Optional.of(state(1, 4)), LongStream.rangeClosed(1, 4).mapToObj(i -> command(i, 1, "v" + i)).toList());
            wal.compact(base(2, 1));
            wal.compact(base(3, 1));
            wal.append(Optional.empty(), List.of(command(5, 1, "v5")));
        }

        try (var wal = open(64)) {
            assertEquals(Optional.of(base(3, 1)), wal.replay().base());
            assertEquals(5, wal.replay().entries().getLast().index());
        }
    }
}
