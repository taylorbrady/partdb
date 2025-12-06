package io.partdb.raft;

import java.util.List;
import java.util.Optional;

public interface RaftStorage extends AutoCloseable {

    InitialState initialState();

    List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes);

    long term(long index);

    long firstIndex();

    long lastIndex();

    void append(HardState hardState, List<LogEntry> entries);

    void sync();

    Optional<Snapshot> snapshot();

    void saveSnapshot(Snapshot snapshot);

    void compact(long index);

    @Override
    void close();

    record InitialState(HardState hardState, Membership membership) {}
}
