package io.partdb.storage.manifest;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;

import java.nio.ByteBuffer;
import java.util.Objects;

public record SSTableInfo(
    long id,
    int level,
    ByteArray smallestKey,
    ByteArray largestKey,
    Timestamp smallestTimestamp,
    Timestamp largestTimestamp,
    long fileSizeBytes,
    long entryCount
) {

    public SSTableInfo {
        Objects.requireNonNull(smallestKey, "smallestKey");
        Objects.requireNonNull(largestKey, "largestKey");
        Objects.requireNonNull(smallestTimestamp, "smallestTimestamp");
        Objects.requireNonNull(largestTimestamp, "largestTimestamp");

        if (id < 0) {
            throw new IllegalArgumentException("id must be non-negative");
        }
        if (level < 0) {
            throw new IllegalArgumentException("level must be non-negative");
        }
        if (fileSizeBytes < 0) {
            throw new IllegalArgumentException("fileSizeBytes must be non-negative");
        }
        if (entryCount < 0) {
            throw new IllegalArgumentException("entryCount must be non-negative");
        }
        if (smallestKey.compareTo(largestKey) > 0) {
            throw new IllegalArgumentException("smallestKey must be <= largestKey");
        }
        if (smallestTimestamp.compareTo(largestTimestamp) > 0) {
            throw new IllegalArgumentException("smallestTimestamp must be <= largestTimestamp");
        }
    }

    public boolean overlaps(ByteArray startKey, ByteArray endKey) {
        boolean afterStart = startKey == null || largestKey.compareTo(startKey) >= 0;
        boolean beforeEnd = endKey == null || smallestKey.compareTo(endKey) <= 0;
        return afterStart && beforeEnd;
    }

    public int serializedSize() {
        return 8 + 4 + 4 + smallestKey.length() + 4 + largestKey.length() + 8 + 8 + 8 + 8;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(id);
        buffer.putInt(level);
        buffer.putInt(smallestKey.length());
        buffer.put(smallestKey.toByteArray());
        buffer.putInt(largestKey.length());
        buffer.put(largestKey.toByteArray());
        buffer.putLong(smallestTimestamp.value());
        buffer.putLong(largestTimestamp.value());
        buffer.putLong(fileSizeBytes);
        buffer.putLong(entryCount);
    }

    public static SSTableInfo readFrom(ByteBuffer buffer) {
        long id = buffer.getLong();
        int level = buffer.getInt();

        int smallestKeySize = buffer.getInt();
        byte[] smallestKeyBytes = new byte[smallestKeySize];
        buffer.get(smallestKeyBytes);
        ByteArray smallestKey = ByteArray.copyOf(smallestKeyBytes);

        int largestKeySize = buffer.getInt();
        byte[] largestKeyBytes = new byte[largestKeySize];
        buffer.get(largestKeyBytes);
        ByteArray largestKey = ByteArray.copyOf(largestKeyBytes);

        Timestamp smallestTimestamp = new Timestamp(buffer.getLong());
        Timestamp largestTimestamp = new Timestamp(buffer.getLong());

        long fileSizeBytes = buffer.getLong();
        long entryCount = buffer.getLong();

        return new SSTableInfo(id, level, smallestKey, largestKey, smallestTimestamp, largestTimestamp, fileSizeBytes, entryCount);
    }
}
