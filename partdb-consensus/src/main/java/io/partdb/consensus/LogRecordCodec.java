package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

final class LogRecordCodec {

    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final byte ENTRY_TYPE_DATA = 0;
    private static final byte ENTRY_TYPE_NOOP = 1;
    private static final byte ENTRY_TYPE_CONFIG = 2;

    private LogRecordCodec() {}

    public static void writeEntry(ByteBuffer buf, RaftLogEntry entry) {
        switch (entry) {
            case RaftLogEntry.Data(long index, long term, Bytes data) -> {
                buf.put(ENTRY_TYPE_DATA);
                buf.putLong(index);
                buf.putLong(term);
                buf.putInt(data.size());
                buf.put(data.asReadOnlyByteBuffer());
            }
            case RaftLogEntry.NoOp(long index, long term) -> {
                buf.put(ENTRY_TYPE_NOOP);
                buf.putLong(index);
                buf.putLong(term);
            }
            case RaftLogEntry.Config(long index, long term, RaftMembership configuration) -> {
                buf.put(ENTRY_TYPE_CONFIG);
                buf.putLong(index);
                buf.putLong(term);
                writeConfiguration(buf, configuration);
            }
        }
    }

    public static RaftLogEntry readEntry(ByteBuffer buf) {
        byte type = buf.get();
        long index = buf.getLong();
        long term = buf.getLong();

        return switch (type) {
            case ENTRY_TYPE_DATA -> {
                int dataLen = buf.getInt();
                byte[] data = new byte[dataLen];
                buf.get(data);
                yield new RaftLogEntry.Data(index, term, Bytes.copyOf(data));
            }
            case ENTRY_TYPE_NOOP -> new RaftLogEntry.NoOp(index, term);
            case ENTRY_TYPE_CONFIG -> {
                RaftMembership configuration = readConfiguration(buf);
                yield new RaftLogEntry.Config(index, term, configuration);
            }
            default -> throw new IllegalArgumentException("Unknown entry type: " + type);
        };
    }

    public static int entrySize(RaftLogEntry entry) {
        return switch (entry) {
            case RaftLogEntry.Data(long index, long term, Bytes data) -> 1 + 8 + 8 + 4 + data.size();
            case RaftLogEntry.NoOp(long index, long term) -> 1 + 8 + 8;
            case RaftLogEntry.Config(long index, long term, RaftMembership configuration) -> 1 + 8 + 8 + configurationSize(configuration);
        };
    }

    public static void writeHardState(ByteBuffer buf, RaftHardState state) {
        buf.putLong(state.term());
        if (state.votedFor() == null) {
            buf.putShort((short) 0);
        } else {
            byte[] votedForBytes = state.votedFor().getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) votedForBytes.length);
            buf.put(votedForBytes);
        }
        buf.putLong(state.commit());
    }

    public static RaftHardState readHardState(ByteBuffer buf) {
        long term = buf.getLong();
        short votedForLen = buf.getShort();
        String votedFor = null;
        if (votedForLen > 0) {
            byte[] votedForBytes = new byte[votedForLen];
            buf.get(votedForBytes);
            votedFor = new String(votedForBytes, StandardCharsets.UTF_8);
        }
        long commit = buf.getLong();
        return new RaftHardState(term, votedFor, commit);
    }

    public static int hardStateSize(RaftHardState state) {
        int size = 8 + 2 + 8;
        if (state.votedFor() != null) {
            size += state.votedFor().getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    public static void writeConfiguration(ByteBuffer buf, RaftMembership configuration) {
        writeStringSet(buf, configuration.voters());
        writeStringSet(buf, configuration.learners());
    }

    public static RaftMembership readConfiguration(ByteBuffer buf) {
        Set<String> voters = readStringSet(buf);
        Set<String> learners = readStringSet(buf);
        return new RaftMembership(voters, learners);
    }

    public static int configurationSize(RaftMembership configuration) {
        return stringSetSize(configuration.voters()) + stringSetSize(configuration.learners());
    }

    private static void writeStringSet(ByteBuffer buf, Set<String> set) {
        buf.putShort((short) set.size());
        for (String s : set) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) bytes.length);
            buf.put(bytes);
        }
    }

    private static Set<String> readStringSet(ByteBuffer buf) {
        short count = buf.getShort();
        Set<String> set = HashSet.newHashSet(count);
        for (int i = 0; i < count; i++) {
            short len = buf.getShort();
            byte[] bytes = new byte[len];
            buf.get(bytes);
            set.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return set;
    }

    private static int stringSetSize(Set<String> set) {
        int size = 2;
        for (String s : set) {
            size += 2 + s.getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }
}
