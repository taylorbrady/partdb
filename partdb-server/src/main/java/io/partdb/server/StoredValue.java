package io.partdb.server;

import io.partdb.common.ByteArray;

import java.nio.ByteBuffer;

public record StoredValue(
    ByteArray value,
    long version,
    long leaseId
) {

    private static final int HEADER_SIZE = 16;

    public ByteArray encode() {
        byte[] valueBytes = value.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + valueBytes.length);
        buffer.putLong(version);
        buffer.putLong(leaseId);
        buffer.put(valueBytes);
        return ByteArray.copyOf(buffer.array());
    }

    public static StoredValue decode(ByteArray encoded) {
        ByteBuffer buffer = ByteBuffer.wrap(encoded.toByteArray());
        long version = buffer.getLong();
        long leaseId = buffer.getLong();
        byte[] valueBytes = new byte[buffer.remaining()];
        buffer.get(valueBytes);
        return new StoredValue(ByteArray.copyOf(valueBytes), version, leaseId);
    }
}
