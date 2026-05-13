package io.partdb.consensus;

import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftLogReader;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

interface RaftStore extends RaftLogReader, AutoCloseable {

    Bootstrap bootstrap();

    /**
     * Appends one Raft persistence batch.
     *
     * <p>If {@code entries} is non-empty, the store must replace any durable
     * suffix beginning at the first new entry index before exposing the new
     * entries. After close and reopen, reads must observe the replacement log
     * and never the stale suffix.</p>
     */
    void append(RaftHardState hardState, List<RaftLogEntry> entries);

    void sync();

    Optional<RaftSnapshot> snapshot();

    void saveSnapshot(RaftSnapshot snapshot);

    void compact(long index);

    @Override
    void close();

    record Bootstrap(Optional<RaftHardState> hardState, Optional<RaftMembership> membership) {
        public Bootstrap {
            hardState = Objects.requireNonNull(hardState, "hardState must not be null");
            membership = Objects.requireNonNull(membership, "membership must not be null");
        }
    }
}
