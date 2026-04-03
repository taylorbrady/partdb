package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PartDbCommandResultCodec {
    private static final byte VERSION_1 = 1;

    private static final byte OP_PUT_APPLIED = 1;
    private static final byte OP_DELETE_APPLIED = 2;
    private static final byte OP_LEASE_GRANTED = 3;
    private static final byte OP_LEASE_KEPT_ALIVE = 4;
    private static final byte OP_LEASE_REVOKED = 5;
    private static final byte OP_LEASE_NOT_FOUND = 6;

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
            case PartDbCommandResult.LeaseGranted(long modRevision, long leaseId, long ttlNanos) -> {
                buffer.put(OP_LEASE_GRANTED);
                buffer.putLong(modRevision);
                buffer.putLong(leaseId);
                buffer.putLong(ttlNanos);
            }
            case PartDbCommandResult.LeaseKeptAlive(long modRevision, long leaseId, long ttlNanos) -> {
                buffer.put(OP_LEASE_KEPT_ALIVE);
                buffer.putLong(modRevision);
                buffer.putLong(leaseId);
                buffer.putLong(ttlNanos);
            }
            case PartDbCommandResult.LeaseRevoked(long modRevision, long leaseId, long deletedKeyCount) -> {
                buffer.put(OP_LEASE_REVOKED);
                buffer.putLong(modRevision);
                buffer.putLong(leaseId);
                buffer.putLong(deletedKeyCount);
            }
            case PartDbCommandResult.LeaseNotFound(long leaseId) -> {
                buffer.put(OP_LEASE_NOT_FOUND);
                buffer.putLong(leaseId);
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
            case OP_LEASE_GRANTED ->
                new PartDbCommandResult.LeaseGranted(buffer.getLong(), buffer.getLong(), buffer.getLong());
            case OP_LEASE_KEPT_ALIVE ->
                new PartDbCommandResult.LeaseKeptAlive(buffer.getLong(), buffer.getLong(), buffer.getLong());
            case OP_LEASE_REVOKED ->
                new PartDbCommandResult.LeaseRevoked(buffer.getLong(), buffer.getLong(), buffer.getLong());
            case OP_LEASE_NOT_FOUND -> new PartDbCommandResult.LeaseNotFound(buffer.getLong());
            default -> throw new IllegalArgumentException("Unknown PartDB command result opcode: " + opcode);
        };
    }

    private static int encodedSize(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.PutApplied _,
                 PartDbCommandResult.DeleteApplied _,
                 PartDbCommandResult.LeaseNotFound _ -> 1 + 1 + 8;
            case PartDbCommandResult.LeaseGranted _,
                 PartDbCommandResult.LeaseKeptAlive _,
                 PartDbCommandResult.LeaseRevoked _ -> 1 + 1 + 8 + 8 + 8;
        };
    }
}
