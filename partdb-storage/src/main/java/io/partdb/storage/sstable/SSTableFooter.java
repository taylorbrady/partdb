package io.partdb.storage.sstable;

import io.partdb.common.Slice;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32C;

record SSTableFooter(
    long bloomFilterOffset,
    int bloomFilterSize,
    long indexOffset,
    int blockCount,
    Slice smallestKey,
    Slice largestKey,
    long smallestRevision,
    long largestRevision,
    long entryCount,
    int checksum
) {

    SSTableFooter {
        Objects.requireNonNull(smallestKey, "smallestKey cannot be null");
        Objects.requireNonNull(largestKey, "largestKey cannot be null");
    }

    byte[] serialize() {
        int footerSize = calculateFooterSize(smallestKey, largestKey);
        ByteBuffer buffer = ByteBuffer.allocate(footerSize).order(ByteOrder.nativeOrder());

        buffer.putLong(bloomFilterOffset);
        buffer.putInt(bloomFilterSize);
        buffer.putLong(indexOffset);
        buffer.putInt(blockCount);

        buffer.putInt(smallestKey.length());
        buffer.put(smallestKey.toByteArray());

        buffer.putInt(largestKey.length());
        buffer.put(largestKey.toByteArray());

        buffer.putLong(smallestRevision);
        buffer.putLong(largestRevision);

        buffer.putLong(entryCount);

        buffer.putInt(footerSize);

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    static SSTableFooter deserialize(MemorySegment segment) {
        long offset = 0;

        long bloomFilterOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;
        int bloomFilterSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;
        long indexOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;
        int blockCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;

        int smallestKeySize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;
        Slice smallestKey = Slice.wrap(segment.asSlice(offset, smallestKeySize));
        offset += smallestKeySize;

        int largestKeySize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;
        Slice largestKey = Slice.wrap(segment.asSlice(offset, largestKeySize));
        offset += largestKeySize;

        long smallestRevision = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;
        long largestRevision = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;

        long entryCount = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += 8;

        int footerSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += 4;

        int expectedChecksum = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);

        byte[] checksumData = segment.asSlice(0, footerSize - 4).toArray(ValueLayout.JAVA_BYTE);
        CRC32C crc = new CRC32C();
        crc.update(checksumData);
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new SSTableException("Footer checksum mismatch");
        }

        return new SSTableFooter(
            bloomFilterOffset, bloomFilterSize, indexOffset, blockCount,
            smallestKey, largestKey, smallestRevision, largestRevision,
            entryCount, expectedChecksum
        );
    }

    static int calculateFooterSize(Slice smallestKey, Slice largestKey) {
        return 8 + 4 + 8 + 4 + 4 + smallestKey.length() + 4 + largestKey.length() + 8 + 8 + 8 + 4 + 4;
    }
}
