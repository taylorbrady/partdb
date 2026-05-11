package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteBatchOperation;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PartDbCommandCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_BATCH_WRITE = 3;

    private static final byte BATCH_OP_PUT = 1;
    private static final byte BATCH_OP_DELETE = 2;

    private PartDbCommandCodec() {
    }

    public static Bytes encode(PartDbCommand command) {
        var buffer = ByteBuffer.allocate(encodedSize(command)).order(ByteOrder.BIG_ENDIAN);
        buffer.put(VERSION_1);

        switch (command) {
            case PartDbCommand.Put(var key, var value) -> {
                buffer.put(OP_PUT);
                putBytes(buffer, key);
                putBytes(buffer, value);
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
                        case WriteBatchOperation.Put(var key, var value) -> {
                            buffer.put(BATCH_OP_PUT);
                            putBytes(buffer, key);
                            putBytes(buffer, value);
                        }
                        case WriteBatchOperation.Delete(var key) -> {
                            buffer.put(BATCH_OP_DELETE);
                            putBytes(buffer, key);
                        }
                    }
                }
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
            case OP_PUT -> new PartDbCommand.Put(readBytes(buffer), readBytes(buffer));
            case OP_DELETE -> new PartDbCommand.Delete(readBytes(buffer));
            case OP_BATCH_WRITE -> new PartDbCommand.BatchWrite(readBatch(buffer));
            default -> throw new IllegalArgumentException("Unknown PartDB command opcode: " + opcode);
        };
    }

    private static int encodedSize(PartDbCommand command) {
        return switch (command) {
            case PartDbCommand.Put(var key, var value) -> 1 + 1 + 4 + key.size() + 4 + value.size();
            case PartDbCommand.Delete(var key) -> 1 + 1 + 4 + key.size();
            case PartDbCommand.BatchWrite(var batch) -> 1 + 1 + 4 + batch.operations().stream()
                .mapToInt(PartDbCommandCodec::encodedSize)
                .sum();
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
            case WriteBatchOperation.Put(var key, var value) -> 1 + 4 + key.size() + 4 + value.size();
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
                    operations.add(WriteBatchOperation.Put.of(key, value));
                }
                case BATCH_OP_DELETE -> operations.add(new WriteBatchOperation.Delete(readBytes(buffer)));
                default -> throw new IllegalArgumentException("Unknown PartDB batch operation opcode: " + opcode);
            }
        }
        return new WriteBatch(operations);
    }
}
