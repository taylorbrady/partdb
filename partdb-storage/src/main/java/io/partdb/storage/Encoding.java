package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

final class Encoding {

    static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ORDER);
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ORDER);

    private Encoding() {
    }

    static ByteBuffer allocate(int size) {
        return ByteBuffer.allocate(size).order(ORDER);
    }

    static ByteBuffer wrap(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ORDER);
    }

    static int crc(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    static int crc(MemorySegment segment) {
        CRC32C crc = new CRC32C();
        crc.update(segment.asByteBuffer());
        return (int) crc.getValue();
    }

    static void check(boolean condition, String message) {
        if (!condition) {
            throw new StoreException.Corruption(message);
        }
    }
}
