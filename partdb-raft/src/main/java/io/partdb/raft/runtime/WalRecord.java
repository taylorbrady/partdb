package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;

import java.nio.ByteBuffer;

sealed interface WalRecord {

    byte ENTRY_COMMAND = 0;
    byte ENTRY_NOOP = 1;
    byte ENTRY_MEMBERSHIP = 2;
    byte STATE = 3;
    byte SNAPSHOT = 4;

    record Entry(RaftLogEntry entry) implements WalRecord {}

    record State(RaftHardState hardState) implements WalRecord {}

    record Snapshot(long index, long term) implements WalRecord {}

    default byte type() {
        return switch (this) {
            case Entry(RaftLogEntry.Command _) -> ENTRY_COMMAND;
            case Entry(RaftLogEntry.NoOp _) -> ENTRY_NOOP;
            case Entry(RaftLogEntry.Membership _) -> ENTRY_MEMBERSHIP;
            case State _ -> STATE;
            case Snapshot _ -> SNAPSHOT;
        };
    }

    default ByteBuffer encode() {
        var buffer = ByteBuffer.allocate(size()).order(BinaryFormat.ORDER);
        switch (this) {
            case Entry(var entry) -> {
                buffer.putLong(entry.index());
                buffer.putLong(entry.term());
                switch (entry) {
                    case RaftLogEntry.Command command -> buffer.put(command.command().asReadOnlyByteBuffer());
                    case RaftLogEntry.Membership membership -> BinaryFormat.putMembership(buffer, membership.membership());
                    case RaftLogEntry.NoOp _ -> {}
                }
            }
            case State(var state) -> {
                buffer.putLong(state.term());
                BinaryFormat.putOptionalString(buffer, state.votedFor());
                buffer.putLong(state.commit());
            }
            case Snapshot(long index, long term) -> {
                buffer.putLong(index);
                buffer.putLong(term);
            }
        }
        return buffer.flip();
    }

    private int size() {
        return switch (this) {
            case Entry(RaftLogEntry.Command command) -> 2 * Long.BYTES + command.command().size();
            case Entry(RaftLogEntry.Membership membership) -> 2 * Long.BYTES + BinaryFormat.membershipSize(membership.membership());
            case Entry(RaftLogEntry.NoOp _) -> 2 * Long.BYTES;
            case State(var state) -> 2 * Long.BYTES + BinaryFormat.optionalStringSize(state.votedFor());
            case Snapshot _ -> 2 * Long.BYTES;
        };
    }

    static WalRecord decode(byte type, ByteBuffer buffer) {
        buffer.order(BinaryFormat.ORDER);
        return switch (type) {
            case ENTRY_COMMAND, ENTRY_NOOP, ENTRY_MEMBERSHIP -> {
                long index = buffer.getLong();
                long term = buffer.getLong();
                yield new Entry(switch (type) {
                    case ENTRY_COMMAND -> new RaftLogEntry.Command(index, term, Bytes.copyOf(buffer));
                    case ENTRY_NOOP -> new RaftLogEntry.NoOp(index, term);
                    default -> new RaftLogEntry.Membership(index, term, BinaryFormat.getMembership(buffer));
                });
            }
            case STATE -> new State(new RaftHardState(buffer.getLong(), BinaryFormat.getOptionalString(buffer), buffer.getLong()));
            case SNAPSHOT -> new Snapshot(buffer.getLong(), buffer.getLong());
            default -> throw new RaftStorageException.Corruption("unknown WAL record type " + type);
        };
    }
}
