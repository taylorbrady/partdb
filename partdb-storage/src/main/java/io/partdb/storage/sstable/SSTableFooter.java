package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;

import java.nio.ByteBuffer;
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
        ByteBuffer buffer = ByteBuffer.allocate(footerSize);

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

    public static SSTableFooter deserialize(ByteBuffer buffer) {
        int startPosition = buffer.position();

        long bloomFilterOffset = buffer.getLong();
        int bloomFilterSize = buffer.getInt();
        long indexOffset = buffer.getLong();
        int blockCount = buffer.getInt();

        int largestKeySize = buffer.getInt();
        byte[] largestKeyBytes = new byte[largestKeySize];
        buffer.get(largestKeyBytes);
        ByteArray largestKey = ByteArray.wrap(largestKeyBytes);

        long entryCount = buffer.getLong();

        int footerSize = buffer.getInt();

        int expectedChecksum = buffer.getInt();

        ByteBuffer checksumBuffer = ByteBuffer.allocate(footerSize - 4);
        checksumBuffer.putLong(bloomFilterOffset);
        checksumBuffer.putInt(bloomFilterSize);
        checksumBuffer.putLong(indexOffset);
        checksumBuffer.putInt(blockCount);
        checksumBuffer.putInt(largestKeySize);
        checksumBuffer.put(largestKeyBytes);
        checksumBuffer.putLong(entryCount);
        checksumBuffer.putInt(footerSize);

        CRC32 crc = new CRC32();
        crc.update(checksumBuffer.array());
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new SSTableException("Footer checksum mismatch");
        }

        return new SSTableFooter(bloomFilterOffset, bloomFilterSize, indexOffset, blockCount, largestKey, entryCount, expectedChecksum);
    }

    public static int calculateFooterSize(ByteArray largestKey) {
        return 8 + 4 + 8 + 4 + 4 + largestKey.size() + 8 + 4 + 4;
    }

    public void validate() {
        ByteBuffer buffer = ByteBuffer.allocate(calculateFooterSize(largestKey) - 4);
        buffer.putLong(bloomFilterOffset);
        buffer.putInt(bloomFilterSize);
        buffer.putLong(indexOffset);
        buffer.putInt(blockCount);
        buffer.putInt(largestKey.size());
        buffer.put(largestKey.toByteArray());
        buffer.putLong(entryCount);
        buffer.putInt(calculateFooterSize(largestKey));

        CRC32 crc = new CRC32();
        crc.update(buffer.array());
        int expected = (int) crc.getValue();

        if (checksum != expected) {
            throw new SSTableException("Footer checksum mismatch");
        }
    }
}
