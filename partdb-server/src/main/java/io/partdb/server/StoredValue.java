package io.partdb.server;

import java.nio.ByteBuffer;

public record StoredValue(
    byte[] value,
    long version,
    long leaseId
) {

    private static final int HEADER_SIZE = 16;

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + value.length);
        buffer.putLong(version);
        buffer.putLong(leaseId);
        buffer.put(value);
        return buffer.array();
    }

    public static StoredValue decode(byte[] encoded) {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        long version = buffer.getLong();
        long leaseId = buffer.getLong();
        byte[] valueBytes = new byte[buffer.remaining()];
        buffer.get(valueBytes);
        return new StoredValue(valueBytes, version, leaseId);
    }
}
