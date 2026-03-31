package io.partdb.node.raft;

import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftLogView;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftPersistentState;
import io.partdb.raft.RaftSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface RaftStore extends RaftLogView, AutoCloseable {

    Bootstrap bootstrap();

    void append(RaftPersistentState persistentState, List<LogEntry> entries);

    void sync();

    Optional<RaftSnapshot> snapshot();

    void saveSnapshot(RaftSnapshot snapshot);

    void compact(long index);

    @Override
    void close();

    record Bootstrap(Optional<RaftPersistentState> persistentState, Optional<RaftMembership> membership) {
        public Bootstrap {
            persistentState = Objects.requireNonNull(persistentState, "persistentState must not be null");
            membership = Objects.requireNonNull(membership, "membership must not be null");
        }
    }
}
