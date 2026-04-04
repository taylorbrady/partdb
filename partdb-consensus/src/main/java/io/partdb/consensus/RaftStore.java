package io.partdb.consensus;

import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftConfiguration;
import io.partdb.raft.RaftLogView;
import io.partdb.raft.RaftPersistentState;
import io.partdb.raft.RaftSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

interface RaftStore extends RaftLogView, AutoCloseable {

    Bootstrap bootstrap();

    void append(RaftPersistentState persistentState, List<LogEntry> entries);

    void sync();

    Optional<RaftSnapshot> snapshot();

    void saveSnapshot(RaftSnapshot snapshot);

    void compact(long index);

    @Override
    void close();

    record Bootstrap(Optional<RaftPersistentState> persistentState, Optional<RaftConfiguration> configuration) {
        public Bootstrap {
            persistentState = Objects.requireNonNull(persistentState, "persistentState must not be null");
            configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        }
    }
}
