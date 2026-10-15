package io.partdb.raft.runtime;

import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

final class BinaryFormat {

    static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final int MAX_STRING_BYTES = 0xFFFF;
    private static final int MAX_SET_SIZE = 0xFFFF;

    private BinaryFormat() {
    }

    static int stringSize(String value) {
        return Short.BYTES + utf8(value).length;
    }

    static void putString(ByteBuffer buffer, String value) {
        byte[] bytes = utf8(value);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    static String getString(ByteBuffer buffer) {
        byte[] bytes = new byte[Short.toUnsignedInt(buffer.getShort())];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static int nodeSize(NodeId node) {
        return stringSize(node.value());
    }

    static void putNode(ByteBuffer buffer, NodeId node) {
        putString(buffer, node.value());
    }

    static NodeId getNode(ByteBuffer buffer) {
        return new NodeId(getString(buffer));
    }

    static int optionalNodeSize(Optional<NodeId> node) {
        return Byte.BYTES + node.map(BinaryFormat::nodeSize).orElse(0);
    }

    static void putOptionalNode(ByteBuffer buffer, Optional<NodeId> node) {
        buffer.put((byte) (node.isPresent() ? 1 : 0));
        node.ifPresent(present -> putNode(buffer, present));
    }

    static Optional<NodeId> getOptionalNode(ByteBuffer buffer) {
        return switch (buffer.get()) {
            case 0 -> Optional.empty();
            case 1 -> Optional.of(getNode(buffer));
            default -> throw new RaftStorageException.Corruption("invalid optional node marker");
        };
    }

    static int membershipSize(Membership membership) {
        return setSize(membership.voters()) + setSize(membership.learners());
    }

    static void putMembership(ByteBuffer buffer, Membership membership) {
        putSet(buffer, membership.voters());
        putSet(buffer, membership.learners());
    }

    static Membership getMembership(ByteBuffer buffer) {
        var voters = getSet(buffer);
        var learners = getSet(buffer);
        return new Membership(voters, learners);
    }

    private static byte[] utf8(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("string exceeds " + MAX_STRING_BYTES + " bytes");
        }
        return bytes;
    }

    private static int setSize(Set<NodeId> values) {
        return Short.BYTES + values.stream().mapToInt(BinaryFormat::nodeSize).sum();
    }

    private static void putSet(ByteBuffer buffer, Set<NodeId> values) {
        if (values.size() > MAX_SET_SIZE) {
            throw new IllegalArgumentException("membership exceeds " + MAX_SET_SIZE + " nodes");
        }
        buffer.putShort((short) values.size());
        values.forEach(value -> putNode(buffer, value));
    }

    private static TreeSet<NodeId> getSet(ByteBuffer buffer) {
        int count = Short.toUnsignedInt(buffer.getShort());
        var values = new TreeSet<NodeId>();
        for (int i = 0; i < count; i++) {
            values.add(getNode(buffer));
        }
        return values;
    }
}
