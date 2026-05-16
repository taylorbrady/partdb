package io.partdb.node.command;

import io.partdb.bytes.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class KvCommandResultCodec {
    private static final byte VERSION_1 = 1;
    private static final byte OP_APPLIED = 1;
    private static final byte OP_CONDITION_FAILED = 2;

    private KvCommandResultCodec() {
    }

    public static Bytes encode(KvCommandResult result) {
        var buffer = ByteBuffer.allocate(encodedSize(result)).order(ByteOrder.BIG_ENDIAN);
        buffer.put(VERSION_1);

        switch (result) {
            case KvCommandResult.Applied(long revision) -> {
                buffer.put(OP_APPLIED);
                buffer.putLong(revision);
            }
            case KvCommandResult.ConditionFailed _ -> buffer.put(OP_CONDITION_FAILED);
        }

        return Bytes.copyOf(buffer.array());
    }

    public static KvCommandResult decode(Bytes encoded) {
        var buffer = encoded.asReadOnlyByteBuffer().order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION_1) {
            throw new IllegalArgumentException("Unsupported KV command result version: " + version);
        }

        byte opcode = buffer.get();
        return switch (opcode) {
            case OP_APPLIED -> new KvCommandResult.Applied(buffer.getLong());
            case OP_CONDITION_FAILED -> new KvCommandResult.ConditionFailed();
            default -> throw new IllegalArgumentException("Unknown KV command result opcode: " + opcode);
        };
    }

    private static int encodedSize(KvCommandResult result) {
        return switch (result) {
            case KvCommandResult.Applied _ -> 1 + 1 + 8;
            case KvCommandResult.ConditionFailed _ -> 1 + 1;
        };
    }
}
