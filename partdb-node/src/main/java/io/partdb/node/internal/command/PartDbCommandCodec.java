package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PartDbCommandCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_GRANT_LEASE = 3;
    private static final byte OP_REVOKE_LEASE = 4;
    private static final byte OP_KEEP_ALIVE_LEASE = 5;
    private static final byte OP_EXPIRE_LEASE = 6;

    private PartDbCommandCodec() {
    }

    public static Bytes encode(PartDbCommand command) {
        var buffer = ByteBuffer.allocate(encodedSize(command)).order(ByteOrder.BIG_ENDIAN);
        buffer.put(VERSION_1);

        switch (command) {
            case PartDbCommand.Put(var key, var value, long leaseId) -> {
                buffer.put(OP_PUT);
                putBytes(buffer, key);
                putBytes(buffer, value);
                buffer.putLong(leaseId);
            }
            case PartDbCommand.Delete(var key) -> {
                buffer.put(OP_DELETE);
                putBytes(buffer, key);
            }
            case PartDbCommand.GrantLease(long ttlNanos) -> {
                buffer.put(OP_GRANT_LEASE);
                buffer.putLong(ttlNanos);
            }
            case PartDbCommand.RevokeLease(long leaseId) -> {
                buffer.put(OP_REVOKE_LEASE);
                buffer.putLong(leaseId);
            }
            case PartDbCommand.KeepAliveLease(long leaseId) -> {
                buffer.put(OP_KEEP_ALIVE_LEASE);
                buffer.putLong(leaseId);
            }
            case PartDbCommand.ExpireLease(long leaseId) -> {
                buffer.put(OP_EXPIRE_LEASE);
                buffer.putLong(leaseId);
            }
        }

        return Bytes.copyOf(buffer.array());
    }

    public static PartDbCommand decode(Bytes encoded) {
        var buffer = encoded.asReadOnlyByteBuffer().order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION_1) {
            throw new IllegalArgumentException("Unsupported PartDB command version: " + version);
        }

        byte opcode = buffer.get();
        return switch (opcode) {
            case OP_PUT -> new PartDbCommand.Put(readBytes(buffer), readBytes(buffer), buffer.getLong());
            case OP_DELETE -> new PartDbCommand.Delete(readBytes(buffer));
            case OP_GRANT_LEASE -> new PartDbCommand.GrantLease(buffer.getLong());
            case OP_REVOKE_LEASE -> new PartDbCommand.RevokeLease(buffer.getLong());
            case OP_KEEP_ALIVE_LEASE -> new PartDbCommand.KeepAliveLease(buffer.getLong());
            case OP_EXPIRE_LEASE -> new PartDbCommand.ExpireLease(buffer.getLong());
            default -> throw new IllegalArgumentException("Unknown PartDB command opcode: " + opcode);
        };
    }

    private static int encodedSize(PartDbCommand command) {
        return switch (command) {
            case PartDbCommand.Put(var key, var value, long ignored) -> 1 + 1 + 4 + key.size() + 4 + value.size() + 8;
            case PartDbCommand.Delete(var key) -> 1 + 1 + 4 + key.size();
            case PartDbCommand.GrantLease _,
                 PartDbCommand.RevokeLease _,
                 PartDbCommand.KeepAliveLease _,
                 PartDbCommand.ExpireLease _ -> 1 + 1 + 8;
        };
    }

    private static void putBytes(ByteBuffer buffer, Bytes bytes) {
        buffer.putInt(bytes.size());
        buffer.put(bytes.asReadOnlyByteBuffer());
    }

    private static Bytes readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid PartDB command byte field length: " + length);
        }
        byte[] data = new byte[length];
        buffer.get(data);
        return Bytes.copyOf(data);
    }
}
