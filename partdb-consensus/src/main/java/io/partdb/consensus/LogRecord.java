package io.partdb.consensus;

import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;

sealed interface LogRecord {

    byte TYPE_ENTRY = 0;
    byte TYPE_STATE = 1;
    byte TYPE_SNAPSHOT_MARKER = 2;

    record Entry(RaftLogEntry entry) implements LogRecord {}

    record State(RaftHardState hardState) implements LogRecord {}

    record SnapshotMarker(long index, long term) implements LogRecord {}
}
