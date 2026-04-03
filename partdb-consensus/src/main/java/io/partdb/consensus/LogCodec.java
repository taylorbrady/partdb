package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftPersistentState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftMembership;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

final class LogCodec {

    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final byte ENTRY_TYPE_DATA = 0;
    private static final byte ENTRY_TYPE_NOOP = 1;
    private static final byte ENTRY_TYPE_CONFIG = 2;

    private LogCodec() {}

    public static void writeEntry(ByteBuffer buf, LogEntry entry) {
        switch (entry) {
            case LogEntry.Data(long index, long term, Bytes data) -> {
                buf.put(ENTRY_TYPE_DATA);
                buf.putLong(index);
                buf.putLong(term);
                buf.putInt(data.size());
                buf.put(data.asReadOnlyByteBuffer());
            }
            case LogEntry.NoOp(long index, long term) -> {
                buf.put(ENTRY_TYPE_NOOP);
                buf.putLong(index);
                buf.putLong(term);
            }
            case LogEntry.Config(long index, long term, RaftMembership membership) -> {
                buf.put(ENTRY_TYPE_CONFIG);
                buf.putLong(index);
                buf.putLong(term);
                writeMembership(buf, membership);
            }
        }
    }

    public static LogEntry readEntry(ByteBuffer buf) {
        byte type = buf.get();
        long index = buf.getLong();
        long term = buf.getLong();

        return switch (type) {
            case ENTRY_TYPE_DATA -> {
                int dataLen = buf.getInt();
                byte[] data = new byte[dataLen];
                buf.get(data);
                yield new LogEntry.Data(index, term, Bytes.copyOf(data));
            }
            case ENTRY_TYPE_NOOP -> new LogEntry.NoOp(index, term);
            case ENTRY_TYPE_CONFIG -> {
                RaftMembership membership = readMembership(buf);
                yield new LogEntry.Config(index, term, membership);
            }
            default -> throw new IllegalArgumentException("Unknown entry type: " + type);
        };
    }

    public static int entrySize(LogEntry entry) {
        return switch (entry) {
            case LogEntry.Data(long index, long term, Bytes data) -> 1 + 8 + 8 + 4 + data.size();
            case LogEntry.NoOp(long index, long term) -> 1 + 8 + 8;
            case LogEntry.Config(long index, long term, RaftMembership membership) -> 1 + 8 + 8 + membershipSize(membership);
        };
    }

    public static void writeHardState(ByteBuffer buf, RaftPersistentState state) {
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

    public static RaftPersistentState readHardState(ByteBuffer buf) {
        long term = buf.getLong();
        short votedForLen = buf.getShort();
        String votedFor = null;
        if (votedForLen > 0) {
            byte[] votedForBytes = new byte[votedForLen];
            buf.get(votedForBytes);
            votedFor = new String(votedForBytes, StandardCharsets.UTF_8);
        }
        long commit = buf.getLong();
        return new RaftPersistentState(term, votedFor, commit);
    }

    public static int hardStateSize(RaftPersistentState state) {
        int size = 8 + 2 + 8;
        if (state.votedFor() != null) {
            size += state.votedFor().getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    public static void writeMembership(ByteBuffer buf, RaftMembership membership) {
        writeStringSet(buf, membership.voters());
        writeStringSet(buf, membership.learners());
    }

    public static RaftMembership readMembership(ByteBuffer buf) {
        Set<String> voters = readStringSet(buf);
        Set<String> learners = readStringSet(buf);
        return new RaftMembership(voters, learners);
    }

    public static int membershipSize(RaftMembership membership) {
        return stringSetSize(membership.voters()) + stringSetSize(membership.learners());
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
