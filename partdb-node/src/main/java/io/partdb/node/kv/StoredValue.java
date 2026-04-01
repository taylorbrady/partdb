package io.partdb.node.kv;

import java.nio.ByteBuffer;

record StoredValue(
    byte[] value,
    long leaseId
) {

    private static final int HEADER_SIZE = 8;

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + value.length);
        buffer.putLong(leaseId);
        buffer.put(value);
        return buffer.array();
    }

    public static StoredValue decode(byte[] encoded) {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        long leaseId = buffer.getLong();
        byte[] valueBytes = new byte[buffer.remaining()];
        buffer.get(valueBytes);
        return new StoredValue(valueBytes, leaseId);
    }
}
