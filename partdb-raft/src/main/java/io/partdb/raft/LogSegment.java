package io.partdb.raft;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

sealed interface LogSegment permits ActiveSegment, SealedSegment {

    Optional<LogEntry> get(long index);

    List<LogEntry> getRange(long startIndex, long endIndex);

    List<LogEntry> readAll();

    long firstIndex();

    long lastIndex();

    long segmentId();

    Path path();

    long size();

    void close();
}
