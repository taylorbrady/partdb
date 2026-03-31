package io.partdb.node.raft;

import io.partdb.raft.RaftPersistentState;
import io.partdb.raft.LogEntry;

public sealed interface LogRecord {

    byte TYPE_ENTRY = 0;
    byte TYPE_STATE = 1;
    byte TYPE_SNAPSHOT_MARKER = 2;

    record Entry(LogEntry entry) implements LogRecord {}

    record State(RaftPersistentState hardState) implements LogRecord {}

    record SnapshotMarker(long index, long term) implements LogRecord {}
}
