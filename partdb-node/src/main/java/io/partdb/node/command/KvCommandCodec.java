package io.partdb.node.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.Condition;
import io.partdb.node.kv.Transaction;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteOperation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public final class KvCommandCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_BATCH_WRITE = 3;
    private static final byte OP_COMPARE_AND_WRITE = 4;

    private static final byte BATCH_OP_PUT = 1;
    private static final byte BATCH_OP_DELETE = 2;

    private static final byte CONDITION_EXISTS = 1;
    private static final byte CONDITION_MISSING = 2;
    private static final byte CONDITION_VALUE_EQUALS = 3;
    private static final byte CONDITION_REVISION_EQUALS = 4;

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
                putOperations(buffer, batch.operations());
            }
            case KvCommand.CompareAndWrite(var transaction) -> {
                buffer.put(OP_COMPARE_AND_WRITE);
                buffer.putInt(transaction.conditions().size());
                for (Condition condition : transaction.conditions()) {
                    putCondition(buffer, condition);
                }
                putOperations(buffer, transaction.operations());
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
            case OP_COMPARE_AND_WRITE -> new KvCommand.CompareAndWrite(readTransaction(buffer));
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
            case KvCommand.CompareAndWrite(var transaction) -> 1 + 1 + 4 + transaction.conditions().stream()
                .mapToInt(KvCommandCodec::encodedSize)
                .sum()
                + 4 + transaction.operations().stream()
                .mapToInt(KvCommandCodec::encodedSize)
                .sum();
        };
    }

    private static int encodedSize(Condition condition) {
        return switch (condition) {
            case Condition.Exists(var key) -> 1 + 4 + key.size();
            case Condition.Missing(var key) -> 1 + 4 + key.size();
            case Condition.ValueEquals(var key, var value) -> 1 + 4 + key.size() + 4 + value.size();
            case Condition.RevisionEquals(var key, long ignored) -> 1 + 4 + key.size() + 8;
        };
    }

    private static int encodedSize(WriteOperation operation) {
        return switch (operation) {
            case WriteOperation.Put(var key, var value) -> 1 + 4 + key.size() + 4 + value.size();
            case WriteOperation.Delete(var key) -> 1 + 4 + key.size();
        };
    }

    private static void putCondition(ByteBuffer buffer, Condition condition) {
        switch (condition) {
            case Condition.Exists(var key) -> {
                buffer.put(CONDITION_EXISTS);
                putBytes(buffer, key);
            }
            case Condition.Missing(var key) -> {
                buffer.put(CONDITION_MISSING);
                putBytes(buffer, key);
            }
            case Condition.ValueEquals(var key, var value) -> {
                buffer.put(CONDITION_VALUE_EQUALS);
                putBytes(buffer, key);
                putBytes(buffer, value);
            }
            case Condition.RevisionEquals(var key, long revision) -> {
                buffer.put(CONDITION_REVISION_EQUALS);
                putBytes(buffer, key);
                buffer.putLong(revision);
            }
        }
    }

    private static void putOperations(ByteBuffer buffer, List<WriteOperation> operations) {
        buffer.putInt(operations.size());
        for (WriteOperation operation : operations) {
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
        return new WriteBatch(readOperations(buffer));
    }

    private static Transaction readTransaction(ByteBuffer buffer) {
        int conditionCount = buffer.getInt();
        if (conditionCount < 0) {
            throw new IllegalArgumentException("Transaction condition count must not be negative");
        }

        var conditions = new ArrayList<Condition>(conditionCount);
        for (int i = 0; i < conditionCount; i++) {
            byte opcode = buffer.get();
            switch (opcode) {
                case CONDITION_EXISTS -> conditions.add(new Condition.Exists(readBytes(buffer)));
                case CONDITION_MISSING -> conditions.add(new Condition.Missing(readBytes(buffer)));
                case CONDITION_VALUE_EQUALS -> conditions.add(new Condition.ValueEquals(readBytes(buffer), readBytes(buffer)));
                case CONDITION_REVISION_EQUALS -> conditions.add(new Condition.RevisionEquals(readBytes(buffer), buffer.getLong()));
                default -> throw new IllegalArgumentException("Unknown KV transaction condition opcode: " + opcode);
            }
        }

        return new Transaction(conditions, readOperations(buffer));
    }

    private static List<WriteOperation> readOperations(ByteBuffer buffer) {
        int count = buffer.getInt();
        if (count <= 0) {
            throw new IllegalArgumentException("Write operation list must contain at least one operation");
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
        return operations;
    }
}
