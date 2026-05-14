package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    private static RaftLogEntry.Data data(long index, long term, String value) {
        return new RaftLogEntry.Data(index, term, Bytes.utf8(value));
    }
}
