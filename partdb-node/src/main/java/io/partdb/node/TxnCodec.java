package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.storage.Mutation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

final class TxnCodec {

    private static final byte VERSION = 1;
    private static final byte EXISTS = 1;
    private static final byte MISSING = 2;
    private static final byte VALUE_EQUALS = 3;
    private static final byte REVISION_EQUALS = 4;
    private static final byte PUT = 1;
    private static final byte DELETE = 2;

    static final class CorruptCommand extends RuntimeException {
        CorruptCommand(String message) {
            super(message);
        }
    }

    private TxnCodec() {
    }

    static Bytes encode(Txn txn) {
        var buffer = ByteBuffer.allocate(size(txn)).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(VERSION);
        buffer.putInt(txn.conditions().size());
        for (Condition condition : txn.conditions()) {
            switch (condition) {
                case Condition.Exists(var key) -> putBytes(buffer.put(EXISTS), key);
                case Condition.Missing(var key) -> putBytes(buffer.put(MISSING), key);
                case Condition.ValueEquals(var key, var value) -> putBytes(putBytes(buffer.put(VALUE_EQUALS), key), value);
                case Condition.RevisionEquals(var key, long revision) -> putBytes(buffer.put(REVISION_EQUALS), key).putLong(revision);
            }
        }
        buffer.putInt(txn.mutations().size());
        for (Mutation mutation : txn.mutations()) {
            switch (mutation) {
                case Mutation.Put(var key, var value) -> putBytes(putBytes(buffer.put(PUT), key), value);
                case Mutation.Delete(var key) -> putBytes(buffer.put(DELETE), key);
            }
        }
        return Bytes.copyOf(buffer.array());
    }

    static Txn decode(Bytes encoded) {
        var buffer = encoded.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        byte version = readByte(buffer);
        if (version != VERSION) {
            throw new CorruptCommand("unsupported command version " + version);
        }
        int conditionCount = readCount(buffer);
        var conditions = new ArrayList<Condition>(conditionCount);
        for (int i = 0; i < conditionCount; i++) {
            byte tag = readByte(buffer);
            Bytes key = readBytes(buffer);
            conditions.add(switch (tag) {
                case EXISTS -> new Condition.Exists(key);
                case MISSING -> new Condition.Missing(key);
                case VALUE_EQUALS -> new Condition.ValueEquals(key, readBytes(buffer));
                case REVISION_EQUALS -> new Condition.RevisionEquals(key, readLong(buffer));
                default -> throw new CorruptCommand("unknown condition tag " + tag);
            });
        }
        int mutationCount = readCount(buffer);
        var mutations = new ArrayList<Mutation>(mutationCount);
        for (int i = 0; i < mutationCount; i++) {
            byte tag = readByte(buffer);
            Bytes key = readBytes(buffer);
            mutations.add(switch (tag) {
                case PUT -> new Mutation.Put(key, readBytes(buffer));
                case DELETE -> new Mutation.Delete(key);
                default -> throw new CorruptCommand("unknown mutation tag " + tag);
            });
        }
        if (buffer.hasRemaining()) {
            throw new CorruptCommand("command has " + buffer.remaining() + " trailing bytes");
        }
        try {
            return new Txn(conditions, mutations);
        } catch (IllegalArgumentException e) {
            throw new CorruptCommand("command is not a valid transaction: " + e.getMessage());
        }
    }

    private static int size(Txn txn) {
        int size = Byte.BYTES + 2 * Integer.BYTES;
        for (Condition condition : txn.conditions()) {
            size += Byte.BYTES + Integer.BYTES + condition.key().size() + switch (condition) {
                case Condition.Exists _, Condition.Missing _ -> 0;
                case Condition.ValueEquals(_, var value) -> Integer.BYTES + value.size();
                case Condition.RevisionEquals _ -> Long.BYTES;
            };
        }
        for (Mutation mutation : txn.mutations()) {
            size += Byte.BYTES + Integer.BYTES + mutation.key().size() + switch (mutation) {
                case Mutation.Put(_, var value) -> Integer.BYTES + value.size();
                case Mutation.Delete _ -> 0;
            };
        }
        return size;
    }

    private static ByteBuffer putBytes(ByteBuffer buffer, Bytes value) {
        return buffer.putInt(value.size()).put(value.asReadOnlyByteBuffer());
    }

    private static byte readByte(ByteBuffer buffer) {
        require(buffer, Byte.BYTES);
        return buffer.get();
    }

    private static long readLong(ByteBuffer buffer) {
        require(buffer, Long.BYTES);
        return buffer.getLong();
    }

    private static int readCount(ByteBuffer buffer) {
        require(buffer, Integer.BYTES);
        int count = buffer.getInt();
        if (count < 0 || count > buffer.remaining()) {
            throw new CorruptCommand("invalid element count " + count);
        }
        return count;
    }

    private static Bytes readBytes(ByteBuffer buffer) {
        require(buffer, Integer.BYTES);
        int length = buffer.getInt();
        if (length < 0) {
            throw new CorruptCommand("negative length " + length);
        }
        require(buffer, length);
        var value = Bytes.copyOf(buffer.slice(buffer.position(), length));
        buffer.position(buffer.position() + length);
        return value;
    }

    private static void require(ByteBuffer buffer, int bytes) {
        if (buffer.remaining() < bytes) {
            throw new CorruptCommand("command is truncated");
        }
    }
}
