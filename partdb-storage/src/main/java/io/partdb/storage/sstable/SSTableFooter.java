package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

public record SSTableFooter(
    long bloomFilterOffset,
    int bloomFilterSize,
    long indexOffset,
    int blockCount,
    ByteArray largestKey,
    long entryCount,
    int checksum
) {

    public static final int MIN_FOOTER_SIZE = 44;

    public SSTableFooter {
        Objects.requireNonNull(largestKey, "largestKey cannot be null");
    }

    public byte[] serialize() {
        int footerSize = calculateFooterSize(largestKey);
        ByteBuffer buffer = ByteBuffer.allocate(footerSize).order(ByteOrder.nativeOrder());

        buffer.putLong(bloomFilterOffset);
        buffer.putInt(bloomFilterSize);
        buffer.putLong(indexOffset);
        buffer.putInt(blockCount);

        buffer.putInt(largestKey.size());
        buffer.put(largestKey.toByteArray());

        buffer.putLong(entryCount);

        buffer.putInt(footerSize);

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    public static SSTableFooter deserialize(MemorySegment segment) {
        long offset = 0;

        long bloomFilterOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;
        int bloomFilterSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;
        long indexOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;
        int blockCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;

        int largestKeySize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;
        byte[] largestKeyBytes = segment.asSlice(offset, largestKeySize).toArray(ValueLayout.JAVA_BYTE);
        offset += largestKeySize;
        ByteArray largestKey = ByteArray.wrap(largestKeyBytes);

        long entryCount = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;

        int footerSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;

        int expectedChecksum = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);

        byte[] checksumData = segment.asSlice(0, footerSize - 4).toArray(ValueLayout.JAVA_BYTE);
        CRC32 crc = new CRC32();
        crc.update(checksumData);
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new SSTableException("Footer checksum mismatch");
        }

        return new SSTableFooter(bloomFilterOffset, bloomFilterSize, indexOffset, blockCount, largestKey, entryCount, expectedChecksum);
    }

    public static int calculateFooterSize(ByteArray largestKey) {
        return 8 + 4 + 8 + 4 + 4 + largestKey.size() + 8 + 4 + 4;
    }
}
