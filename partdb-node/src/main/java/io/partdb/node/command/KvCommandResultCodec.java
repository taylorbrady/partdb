package io.partdb.node.command;

import io.partdb.bytes.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class KvCommandResultCodec {
    private static final byte VERSION_1 = 1;
    private static final byte OP_APPLIED = 1;

    private KvCommandResultCodec() {
    }

    public static Bytes encode(KvCommandResult result) {
        var buffer = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN);
        buffer.put(VERSION_1);

        switch (result) {
            case KvCommandResult.Applied(long revision) -> {
                buffer.put(OP_APPLIED);
                buffer.putLong(revision);
            }
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
            default -> throw new IllegalArgumentException("Unknown KV command result opcode: " + opcode);
        };
    }
}
