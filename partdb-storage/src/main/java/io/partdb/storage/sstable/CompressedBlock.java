package io.partdb.storage.sstable;

import io.partdb.storage.StorageException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

record CompressedBlock(byte codecId, int uncompressedSize, byte[] data) {

    static final int HEADER_SIZE = 9;
    static final int CHECKSUM_SIZE = 4;

    byte[] serialize() {
        int totalSize = HEADER_SIZE + data.length + CHECKSUM_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.nativeOrder());

        buffer.put(codecId);
        buffer.putInt(uncompressedSize);
        buffer.putInt(data.length);
        buffer.put(data);

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    static CompressedBlock deserialize(MemorySegment segment) {
        long size = segment.byteSize();
        if (size < HEADER_SIZE + CHECKSUM_SIZE) {
            throw new StorageException.Corruption("CompressedBlock too small: " + size);
        }

        int storedChecksum = segment.get(
            ValueLayout.JAVA_INT_UNALIGNED,
            size - CHECKSUM_SIZE
        );

        CRC32C crc = new CRC32C();
        byte[] checksumData = segment.asSlice(0, size - CHECKSUM_SIZE)
            .toArray(ValueLayout.JAVA_BYTE);
        crc.update(checksumData);
        int computedChecksum = (int) crc.getValue();

        if (storedChecksum != computedChecksum) {
            throw new StorageException.Corruption("CompressedBlock checksum mismatch");
        }

        byte codecId = segment.get(ValueLayout.JAVA_BYTE, 0);
        int uncompressedSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 1);
        int compressedSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 5);

        byte[] data = segment.asSlice(HEADER_SIZE, compressedSize)
            .toArray(ValueLayout.JAVA_BYTE);

        return new CompressedBlock(codecId, uncompressedSize, data);
    }
}
