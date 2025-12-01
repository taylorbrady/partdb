package io.partdb.raft;

import io.partdb.common.ByteArray;
import io.partdb.common.statemachine.Delete;
import io.partdb.common.statemachine.GrantLease;
import io.partdb.common.statemachine.KeepAliveLease;
import io.partdb.common.statemachine.Operation;
import io.partdb.common.statemachine.Put;
import io.partdb.common.statemachine.RevokeLease;

import java.nio.ByteBuffer;

final class LogEntryCodec {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_GRANT_LEASE = 3;
    private static final byte OP_REVOKE_LEASE = 4;
    private static final byte OP_KEEP_ALIVE_LEASE = 5;

    private LogEntryCodec() {}

    static byte[] serialize(LogEntry entry) {
        Operation op = entry.command();
        return switch (op) {
            case Put put -> serializePut(entry.term(), put);
            case Delete delete -> serializeDelete(entry.term(), delete);
            case GrantLease grantLease -> serializeGrantLease(entry.term(), grantLease);
            case RevokeLease revokeLease -> serializeRevokeLease(entry.term(), revokeLease);
            case KeepAliveLease keepAliveLease -> serializeKeepAliveLease(entry.term(), keepAliveLease);
        };
    }

    static LogEntry deserialize(long index, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long term = buffer.getLong();
        byte opType = buffer.get();

        Operation operation = switch (opType) {
            case OP_PUT -> deserializePut(buffer);
            case OP_DELETE -> deserializeDelete(buffer);
            case OP_GRANT_LEASE -> deserializeGrantLease(buffer);
            case OP_REVOKE_LEASE -> deserializeRevokeLease(buffer);
            case OP_KEEP_ALIVE_LEASE -> deserializeKeepAliveLease(buffer);
            default -> throw new RaftException.LogException("Unknown operation type: " + opType);
        };

        return new LogEntry(index, term, operation);
    }

    private static byte[] serializePut(long term, Put put) {
        int size = 8 + 1 + 4 + put.key().size() + 4 + put.value().size() + 8;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(term);
        buf.put(OP_PUT);
        buf.putInt(put.key().size());
        buf.put(put.key().toByteArray());
        buf.putInt(put.value().size());
        buf.put(put.value().toByteArray());
        buf.putLong(put.leaseId());
        return buf.array();
    }

    private static Put deserializePut(ByteBuffer buffer) {
        int keySize = buffer.getInt();
        byte[] keyBytes = new byte[keySize];
        buffer.get(keyBytes);
        ByteArray key = ByteArray.wrap(keyBytes);

        int valueSize = buffer.getInt();
        byte[] valueBytes = new byte[valueSize];
        buffer.get(valueBytes);
        ByteArray value = ByteArray.wrap(valueBytes);

        long leaseId = buffer.getLong();

        return new Put(key, value, leaseId);
    }

    private static byte[] serializeDelete(long term, Delete delete) {
        int size = 8 + 1 + 4 + delete.key().size();
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(term);
        buf.put(OP_DELETE);
        buf.putInt(delete.key().size());
        buf.put(delete.key().toByteArray());
        return buf.array();
    }

    private static Delete deserializeDelete(ByteBuffer buffer) {
        int keySize = buffer.getInt();
        byte[] keyBytes = new byte[keySize];
        buffer.get(keyBytes);
        ByteArray key = ByteArray.wrap(keyBytes);

        return new Delete(key);
    }

    private static byte[] serializeGrantLease(long term, GrantLease grantLease) {
        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + 8 + 8 + 8);
        buf.putLong(term);
        buf.put(OP_GRANT_LEASE);
        buf.putLong(grantLease.leaseId());
        buf.putLong(grantLease.ttlMillis());
        buf.putLong(grantLease.grantedAtMillis());
        return buf.array();
    }

    private static GrantLease deserializeGrantLease(ByteBuffer buffer) {
        long leaseId = buffer.getLong();
        long ttlMillis = buffer.getLong();
        long grantedAtMillis = buffer.getLong();

        return new GrantLease(leaseId, ttlMillis, grantedAtMillis);
    }

    private static byte[] serializeRevokeLease(long term, RevokeLease revokeLease) {
        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + 8);
        buf.putLong(term);
        buf.put(OP_REVOKE_LEASE);
        buf.putLong(revokeLease.leaseId());
        return buf.array();
    }

    private static RevokeLease deserializeRevokeLease(ByteBuffer buffer) {
        long leaseId = buffer.getLong();

        return new RevokeLease(leaseId);
    }

    private static byte[] serializeKeepAliveLease(long term, KeepAliveLease keepAliveLease) {
        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + 8);
        buf.putLong(term);
        buf.put(OP_KEEP_ALIVE_LEASE);
        buf.putLong(keepAliveLease.leaseId());
        return buf.array();
    }

    private static KeepAliveLease deserializeKeepAliveLease(ByteBuffer buffer) {
        long leaseId = buffer.getLong();

        return new KeepAliveLease(leaseId);
    }
}
