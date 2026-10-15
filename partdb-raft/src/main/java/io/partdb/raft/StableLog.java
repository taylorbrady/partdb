package io.partdb.raft;

import java.util.List;

public interface StableLog {
    long firstIndex();

    long lastIndex();

    long term(long index);

    List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes);
}
