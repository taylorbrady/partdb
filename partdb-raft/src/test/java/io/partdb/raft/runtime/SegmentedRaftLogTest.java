package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentedRaftLogTest {

    @TempDir
    Path tempDir;

    @Test
    void appendReplacesSuffixAcrossSealedSegments() {
        try (var wal = SegmentedRaftLog.create(tempDir, 1)) {
            wal.append(null, List.of(
                data(1, 1, "a"),
                data(2, 1, "old-b"),
                data(3, 1, "old-c")
            ));

            wal.append(null, List.of(data(2, 2, "new-b")));
            wal.sync();
        }

        try (var reopened = SegmentedRaftLog.open(tempDir, 1)) {
            assertEquals(1, reopened.firstIndex());
            assertEquals(2, reopened.lastIndex());
            assertEquals(0, reopened.term(3));
            assertEquals(List.of(data(1, 1, "a"), data(2, 2, "new-b")),
                reopened.entries(1, 4, Long.MAX_VALUE));
        }
    }

    @Test
    void resetStartsLogAfterSnapshotAndKeepsHardState() {
        var hardState = new RaftHardState(3, "n2", 2);
        try (var wal = SegmentedRaftLog.create(tempDir, 1)) {
            wal.append(hardState, List.of(data(1, 1, "a"), data(2, 1, "b"), data(3, 1, "c")));
            wal.reset(5);
            wal.append(null, List.of(data(6, 3, "f")));
            wal.sync();
        }

        try (var reopened = SegmentedRaftLog.open(tempDir, 1)) {
            assertEquals(hardState, reopened.hardState());
            assertEquals(6, reopened.firstIndex());
            assertEquals(6, reopened.lastIndex());
            assertEquals(List.of(data(6, 3, "f")), reopened.entries(6, 7, Long.MAX_VALUE));
        }
    }

    @Test
    void recoveryDiscardsSegmentsLeftBehindByInterruptedReset() throws IOException {
        Path stale = tempDir.resolve("stale");
        try (var wal = SegmentedRaftLog.create(tempDir, 1)) {
            wal.append(new RaftHardState(1, null, 0), List.of(data(1, 1, "a"), data(2, 1, "b")));
            wal.sync();
            Files.createDirectories(stale);
            try (var files = Files.list(tempDir)) {
                for (Path file : files.filter(path -> path.toString().endsWith(".log")).toList()) {
                    Files.copy(file, stale.resolve(file.getFileName()));
                }
            }
            wal.reset(9);
            wal.sync();
        }
        try (var files = Files.list(stale)) {
            for (Path file : files.toList()) {
                Files.move(file, tempDir.resolve(file.getFileName()));
            }
        }

        try (var reopened = SegmentedRaftLog.open(tempDir, 1)) {
            assertEquals(10, reopened.firstIndex());
            assertEquals(9, reopened.lastIndex());
            assertEquals(List.of(), reopened.entries(10, 11, Long.MAX_VALUE));
        }
        try (var files = Files.list(tempDir)) {
            assertEquals(1, files.filter(path -> path.toString().endsWith(".log")).count());
        }
    }

    private static RaftLogEntry.Data data(long index, long term, String value) {
        return new RaftLogEntry.Data(index, term, Bytes.utf8(value));
    }
}
