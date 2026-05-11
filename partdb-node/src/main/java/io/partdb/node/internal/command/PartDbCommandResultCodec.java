package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PartDbCommandResultCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT_APPLIED = 1;
    private static final byte OP_DELETE_APPLIED = 2;
    private static final byte OP_BATCH_WRITE_APPLIED = 3;

    private PartDbCommandResultCodec() {
    }

    public static Bytes encode(PartDbCommandResult result) {
        var buffer = ByteBuffer.allocate(encodedSize(result)).order(ByteOrder.BIG_ENDIAN);
        buffer.put(VERSION_1);

        switch (result) {
            case PartDbCommandResult.PutApplied(long modRevision) -> {
                buffer.put(OP_PUT_APPLIED);
                buffer.putLong(modRevision);
            }
            case PartDbCommandResult.DeleteApplied(long modRevision) -> {
                buffer.put(OP_DELETE_APPLIED);
                buffer.putLong(modRevision);
            }
            case PartDbCommandResult.BatchWriteApplied(long modRevision) -> {
                buffer.put(OP_BATCH_WRITE_APPLIED);
                buffer.putLong(modRevision);
            }
        }

        return Bytes.copyOf(buffer.array());
    }

    public static PartDbCommandResult decode(Bytes encoded) {
        var buffer = encoded.asReadOnlyByteBuffer().order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION_1) {
            throw new IllegalArgumentException("Unsupported PartDB command result version: " + version);
        }

        byte opcode = buffer.get();
        return switch (opcode) {
            case OP_PUT_APPLIED -> new PartDbCommandResult.PutApplied(buffer.getLong());
            case OP_DELETE_APPLIED -> new PartDbCommandResult.DeleteApplied(buffer.getLong());
            case OP_BATCH_WRITE_APPLIED -> new PartDbCommandResult.BatchWriteApplied(buffer.getLong());
            default -> throw new IllegalArgumentException("Unknown PartDB command result opcode: " + opcode);
        };
    }

    private static int encodedSize(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.PutApplied _,
                 PartDbCommandResult.DeleteApplied _,
                 PartDbCommandResult.BatchWriteApplied _ -> 1 + 1 + 8;
        };
    }
}
