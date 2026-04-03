package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteBatchOperation;
import io.partdb.node.lease.LeaseId;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PartDbCommandCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_BATCH_WRITE = 3;
    private static final byte OP_GRANT_LEASE = 4;
    private static final byte OP_REVOKE_LEASE = 5;
    private static final byte OP_KEEP_ALIVE_LEASE = 6;
    private static final byte OP_EXPIRE_LEASE = 7;

    private static final byte BATCH_OP_PUT = 1;
    private static final byte BATCH_OP_DELETE = 2;

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
            case PartDbCommand.BatchWrite(var batch) -> {
                buffer.put(OP_BATCH_WRITE);
                buffer.putInt(batch.operations().size());
                for (WriteBatchOperation operation : batch.operations()) {
                    switch (operation) {
                        case WriteBatchOperation.Put(var key, var value, var leaseId) -> {
                            buffer.put(BATCH_OP_PUT);
                            putBytes(buffer, key);
                            putBytes(buffer, value);
                            buffer.putLong(leaseId.map(LeaseId::value).orElse(0L));
                        }
                        case WriteBatchOperation.Delete(var key) -> {
                            buffer.put(BATCH_OP_DELETE);
                            putBytes(buffer, key);
                        }
                    }
                }
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
            case OP_BATCH_WRITE -> new PartDbCommand.BatchWrite(readBatch(buffer));
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
            case PartDbCommand.BatchWrite(var batch) -> 1 + 1 + 4 + batch.operations().stream()
                .mapToInt(PartDbCommandCodec::encodedSize)
                .sum();
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

    private static int encodedSize(WriteBatchOperation operation) {
        return switch (operation) {
            case WriteBatchOperation.Put(var key, var value, var ignored) -> 1 + 4 + key.size() + 4 + value.size() + 8;
            case WriteBatchOperation.Delete(var key) -> 1 + 4 + key.size();
        };
    }

    private static WriteBatch readBatch(ByteBuffer buffer) {
        int count = buffer.getInt();
        if (count <= 0) {
            throw new IllegalArgumentException("Batch write must contain at least one operation");
        }

        var operations = new ArrayList<WriteBatchOperation>(count);
        for (int i = 0; i < count; i++) {
            byte opcode = buffer.get();
            switch (opcode) {
                case BATCH_OP_PUT -> {
                    Bytes key = readBytes(buffer);
                    Bytes value = readBytes(buffer);
                    long leaseId = buffer.getLong();
                    operations.add(leaseId == 0
                        ? WriteBatchOperation.Put.of(key, value)
                        : WriteBatchOperation.Put.of(key, value, LeaseId.of(leaseId)));
                }
                case BATCH_OP_DELETE -> operations.add(new WriteBatchOperation.Delete(readBytes(buffer)));
                default -> throw new IllegalArgumentException("Unknown PartDB batch operation opcode: " + opcode);
            }
        }
        return new WriteBatch(operations);
    }
}
