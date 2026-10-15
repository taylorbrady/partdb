package io.partdb.raft.runtime;

import io.partdb.raft.RaftMembership;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

final class BinaryFormat {

    static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    private BinaryFormat() {
    }

    static int stringSize(String value) {
        return Short.BYTES + value.getBytes(StandardCharsets.UTF_8).length;
    }

    static void putString(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    static String getString(ByteBuffer buffer) {
        byte[] bytes = new byte[Short.toUnsignedInt(buffer.getShort())];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static int optionalStringSize(Optional<String> value) {
        return Byte.BYTES + value.map(BinaryFormat::stringSize).orElse(0);
    }

    static void putOptionalString(ByteBuffer buffer, Optional<String> value) {
        buffer.put((byte) (value.isPresent() ? 1 : 0));
        value.ifPresent(present -> putString(buffer, present));
    }

    static Optional<String> getOptionalString(ByteBuffer buffer) {
        return buffer.get() == 0 ? Optional.empty() : Optional.of(getString(buffer));
    }

    static int membershipSize(RaftMembership membership) {
        return setSize(membership.voters()) + setSize(membership.learners());
    }

    static void putMembership(ByteBuffer buffer, RaftMembership membership) {
        putSet(buffer, membership.voters());
        putSet(buffer, membership.learners());
    }

    static RaftMembership getMembership(ByteBuffer buffer) {
        var voters = getSet(buffer);
        var learners = getSet(buffer);
        return new RaftMembership(voters, learners);
    }

    private static int setSize(Set<String> values) {
        return Short.BYTES + values.stream().mapToInt(BinaryFormat::stringSize).sum();
    }

    private static void putSet(ByteBuffer buffer, Set<String> values) {
        buffer.putShort((short) values.size());
        values.forEach(value -> putString(buffer, value));
    }

    private static TreeSet<String> getSet(ByteBuffer buffer) {
        int count = Short.toUnsignedInt(buffer.getShort());
        var values = new TreeSet<String>();
        for (int i = 0; i < count; i++) {
            values.add(getString(buffer));
        }
        return values;
    }
}
