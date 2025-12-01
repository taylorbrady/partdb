package io.partdb.raft;

import java.util.Optional;
import java.util.SequencedCollection;

public interface RaftLog extends AutoCloseable {
    long append(LogEntry entry);

    Optional<LogEntry> get(long index);

    SequencedCollection<LogEntry> getRange(long startIndex, long endIndex);

    long firstIndex();

    long lastIndex();

    long lastTerm();

    long sizeInBytes();

    void truncateAfter(long index);

    void deleteBefore(long index);

    void sync();

    @Override
    void close();
}
