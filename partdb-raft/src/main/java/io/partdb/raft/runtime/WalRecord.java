package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.SnapshotMetadata;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

sealed interface WalRecord {

    byte ENTRY_COMMAND = 0;
    byte ENTRY_NOOP = 1;
    byte ENTRY_CONFIGURATION = 2;
    byte STATE = 3;
    byte RESET = 4;
    byte COMPACTED = 5;

    record Entry(LogEntry entry) implements WalRecord {}

    record State(HardState hardState) implements WalRecord {}

    record Reset(SnapshotMetadata base) implements WalRecord {}

    record Compacted(SnapshotMetadata base) implements WalRecord {}

    default byte type() {
        return switch (this) {
            case Entry(LogEntry.Command _) -> ENTRY_COMMAND;
            case Entry(LogEntry.NoOp _) -> ENTRY_NOOP;
            case Entry(LogEntry.Configuration _) -> ENTRY_CONFIGURATION;
            case State _ -> STATE;
            case Reset _ -> RESET;
            case Compacted _ -> COMPACTED;
        };
    }

    default ByteBuffer encode() {
        var buffer = ByteBuffer.allocate(size()).order(BinaryFormat.ORDER);
        switch (this) {
            case Entry(var entry) -> {
                buffer.putLong(entry.index());
                buffer.putLong(entry.term());
                switch (entry) {
                    case LogEntry.Command command -> buffer.put(command.command().asReadOnlyByteBuffer());
                    case LogEntry.Configuration configuration -> BinaryFormat.putMembership(buffer, configuration.membership());
                    case LogEntry.NoOp _ -> {}
                }
            }
            case State(var state) -> {
                buffer.putLong(state.term());
                BinaryFormat.putOptionalNode(buffer, state.votedFor());
                buffer.putLong(state.commit());
            }
            case Reset(var base) -> putBase(buffer, base);
            case Compacted(var base) -> putBase(buffer, base);
        }
        return buffer.flip();
    }

    private int size() {
        return switch (this) {
            case Entry(LogEntry.Command command) -> 2 * Long.BYTES + command.command().size();
            case Entry(LogEntry.Configuration configuration) -> 2 * Long.BYTES + BinaryFormat.membershipSize(configuration.membership());
            case Entry(LogEntry.NoOp _) -> 2 * Long.BYTES;
            case State(var state) -> 2 * Long.BYTES + BinaryFormat.optionalNodeSize(state.votedFor());
            case Reset(var base) -> baseSize(base);
            case Compacted(var base) -> baseSize(base);
        };
    }

    static WalRecord decode(byte type, ByteBuffer buffer) {
        buffer.order(BinaryFormat.ORDER);
        try {
            var record = switch (type) {
                case ENTRY_COMMAND, ENTRY_NOOP, ENTRY_CONFIGURATION -> {
                    long index = buffer.getLong();
                    long term = buffer.getLong();
                    yield new Entry(switch (type) {
                        case ENTRY_COMMAND -> {
                            var command = Bytes.copyOf(buffer);
                            buffer.position(buffer.limit());
                            yield new LogEntry.Command(index, term, command);
                        }
                        case ENTRY_NOOP -> new LogEntry.NoOp(index, term);
                        default -> new LogEntry.Configuration(index, term, BinaryFormat.getMembership(buffer));
                    });
                }
                case STATE -> new State(new HardState(buffer.getLong(), BinaryFormat.getOptionalNode(buffer), buffer.getLong()));
                case RESET -> new Reset(getBase(buffer));
                case COMPACTED -> new Compacted(getBase(buffer));
                default -> throw new RaftStorageException.Corruption("unknown WAL record type " + type);
            };
            if (buffer.hasRemaining()) {
                throw new RaftStorageException.Corruption("WAL record of type " + type + " has " + buffer.remaining() + " trailing bytes");
            }
            return record;
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new RaftStorageException.Corruption("malformed WAL record of type " + type + ": " + e.getMessage());
        }
    }

    private static int baseSize(SnapshotMetadata base) {
        return 2 * Long.BYTES + BinaryFormat.membershipSize(base.membership());
    }

    private static void putBase(ByteBuffer buffer, SnapshotMetadata base) {
        buffer.putLong(base.index());
        buffer.putLong(base.term());
        BinaryFormat.putMembership(buffer, base.membership());
    }

    private static SnapshotMetadata getBase(ByteBuffer buffer) {
        long index = buffer.getLong();
        long term = buffer.getLong();
        return new SnapshotMetadata(index, term, BinaryFormat.getMembership(buffer));
    }
}
