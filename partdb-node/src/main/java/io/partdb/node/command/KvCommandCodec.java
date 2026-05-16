package io.partdb.node.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteOperation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public final class KvCommandCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_BATCH_WRITE = 3;

    private static final byte BATCH_OP_PUT = 1;
    private static final byte BATCH_OP_DELETE = 2;

    private KvCommandCodec() {
    }

    public static Bytes encode(KvCommand command) {
        var buffer = ByteBuffer.allocate(encodedSize(command)).order(ByteOrder.BIG_ENDIAN);
        buffer.put(VERSION_1);

        switch (command) {
            case KvCommand.Put(var key, var value) -> {
                buffer.put(OP_PUT);
                putBytes(buffer, key);
                putBytes(buffer, value);
            }
            case KvCommand.Delete(var key) -> {
                buffer.put(OP_DELETE);
                putBytes(buffer, key);
            }
            case KvCommand.BatchWrite(var batch) -> {
                buffer.put(OP_BATCH_WRITE);
                buffer.putInt(batch.operations().size());
                for (WriteOperation operation : batch.operations()) {
                    switch (operation) {
                        case WriteOperation.Put(var key, var value) -> {
                            buffer.put(BATCH_OP_PUT);
                            putBytes(buffer, key);
                            putBytes(buffer, value);
                        }
                        case WriteOperation.Delete(var key) -> {
                            buffer.put(BATCH_OP_DELETE);
                            putBytes(buffer, key);
                        }
                    }
                }
            }
        }

        return Bytes.copyOf(buffer.array());
    }

    public static KvCommand decode(Bytes encoded) {
        var buffer = encoded.asReadOnlyByteBuffer().order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION_1) {
            throw new IllegalArgumentException("Unsupported KV command version: " + version);
        }

        byte opcode = buffer.get();
        return switch (opcode) {
            case OP_PUT -> new KvCommand.Put(readBytes(buffer), readBytes(buffer));
            case OP_DELETE -> new KvCommand.Delete(readBytes(buffer));
            case OP_BATCH_WRITE -> new KvCommand.BatchWrite(readBatch(buffer));
            default -> throw new IllegalArgumentException("Unknown KV command opcode: " + opcode);
        };
    }

    private static int encodedSize(KvCommand command) {
        return switch (command) {
            case KvCommand.Put(var key, var value) -> 1 + 1 + 4 + key.size() + 4 + value.size();
            case KvCommand.Delete(var key) -> 1 + 1 + 4 + key.size();
            case KvCommand.BatchWrite(var batch) -> 1 + 1 + 4 + batch.operations().stream()
                .mapToInt(KvCommandCodec::encodedSize)
                .sum();
        };
    }

    private static int encodedSize(WriteOperation operation) {
        return switch (operation) {
            case WriteOperation.Put(var key, var value) -> 1 + 4 + key.size() + 4 + value.size();
            case WriteOperation.Delete(var key) -> 1 + 4 + key.size();
        };
    }

    private static void putBytes(ByteBuffer buffer, Bytes bytes) {
        buffer.putInt(bytes.size());
        buffer.put(bytes.asReadOnlyByteBuffer());
    }

    private static Bytes readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid KV command byte field length: " + length);
        }
        byte[] data = new byte[length];
        buffer.get(data);
        return Bytes.copyOf(data);
    }

    private static WriteBatch readBatch(ByteBuffer buffer) {
        int count = buffer.getInt();
        if (count <= 0) {
            throw new IllegalArgumentException("Batch write must contain at least one operation");
        }

        var operations = new ArrayList<WriteOperation>(count);
        for (int i = 0; i < count; i++) {
            byte opcode = buffer.get();
            switch (opcode) {
                case BATCH_OP_PUT -> operations.add(new WriteOperation.Put(readBytes(buffer), readBytes(buffer)));
                case BATCH_OP_DELETE -> operations.add(new WriteOperation.Delete(readBytes(buffer)));
                default -> throw new IllegalArgumentException("Unknown KV batch operation opcode: " + opcode);
            }
        }
        return new WriteBatch(operations);
    }
}
