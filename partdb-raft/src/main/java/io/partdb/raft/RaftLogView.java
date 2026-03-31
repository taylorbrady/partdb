package io.partdb.raft;

import java.util.List;

public interface RaftLogView {

    List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes);

    long term(long index);

    long firstIndex();

    long lastIndex();
}
