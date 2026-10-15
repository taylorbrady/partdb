package io.partdb.raft.runtime;

import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;

sealed interface LogRecord {

    byte TYPE_ENTRY = 0;
    byte TYPE_STATE = 1;

    record Entry(RaftLogEntry entry) implements LogRecord {}

    record State(RaftHardState hardState) implements LogRecord {}
}
