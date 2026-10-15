package io.partdb.raft;

import java.util.List;

public interface RaftLog {
    long firstIndex();

    long lastIndex();

    long term(long index);

    List<RaftLogEntry> entries(long fromIndex, long toIndex, long maxBytes);
}
