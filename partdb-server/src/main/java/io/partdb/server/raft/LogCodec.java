package io.partdb.server.raft;

import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Membership;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public final class LogCodec {

    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final byte ENTRY_TYPE_DATA = 0;
    private static final byte ENTRY_TYPE_NOOP = 1;
    private static final byte ENTRY_TYPE_CONFIG = 2;

    private LogCodec() {}

    public static void writeEntry(ByteBuffer buf, LogEntry entry) {
        switch (entry) {
            case LogEntry.Data(long index, long term, byte[] data) -> {
                buf.put(ENTRY_TYPE_DATA);
                buf.putLong(index);
                buf.putLong(term);
                buf.putInt(data.length);
                buf.put(data);
            }
            case LogEntry.NoOp(long index, long term) -> {
                buf.put(ENTRY_TYPE_NOOP);
                buf.putLong(index);
                buf.putLong(term);
            }
            case LogEntry.Config(long index, long term, Membership membership) -> {
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
                yield new LogEntry.Data(index, term, data);
            }
            case ENTRY_TYPE_NOOP -> new LogEntry.NoOp(index, term);
            case ENTRY_TYPE_CONFIG -> {
                Membership membership = readMembership(buf);
                yield new LogEntry.Config(index, term, membership);
            }
            default -> throw new IllegalArgumentException("Unknown entry type: " + type);
        };
    }

    public static int entrySize(LogEntry entry) {
        return switch (entry) {
            case LogEntry.Data(long index, long term, byte[] data) -> 1 + 8 + 8 + 4 + data.length;
            case LogEntry.NoOp(long index, long term) -> 1 + 8 + 8;
            case LogEntry.Config(long index, long term, Membership membership) -> 1 + 8 + 8 + membershipSize(membership);
        };
    }

    public static void writeHardState(ByteBuffer buf, HardState state) {
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

    public static HardState readHardState(ByteBuffer buf) {
        long term = buf.getLong();
        short votedForLen = buf.getShort();
        String votedFor = null;
        if (votedForLen > 0) {
            byte[] votedForBytes = new byte[votedForLen];
            buf.get(votedForBytes);
            votedFor = new String(votedForBytes, StandardCharsets.UTF_8);
        }
        long commit = buf.getLong();
        return new HardState(term, votedFor, commit);
    }

    public static int hardStateSize(HardState state) {
        int size = 8 + 2 + 8;
        if (state.votedFor() != null) {
            size += state.votedFor().getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    public static void writeMembership(ByteBuffer buf, Membership membership) {
        writeStringSet(buf, membership.voters());
        writeStringSet(buf, membership.learners());
    }

    public static Membership readMembership(ByteBuffer buf) {
        Set<String> voters = readStringSet(buf);
        Set<String> learners = readStringSet(buf);
        return new Membership(voters, learners);
    }

    public static int membershipSize(Membership membership) {
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
