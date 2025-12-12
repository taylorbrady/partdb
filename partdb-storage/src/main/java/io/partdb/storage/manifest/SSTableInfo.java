package io.partdb.storage.manifest;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public record SSTableInfo(
    long id,
    int level,
    byte[] smallestKey,
    byte[] largestKey,
    long smallestRevision,
    long largestRevision,
    long fileSizeBytes,
    long entryCount
) {

    public SSTableInfo {
        Objects.requireNonNull(smallestKey, "smallestKey");
        Objects.requireNonNull(largestKey, "largestKey");

        smallestKey = smallestKey.clone();
        largestKey = largestKey.clone();

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
        if (Arrays.compareUnsigned(smallestKey, largestKey) > 0) {
            throw new IllegalArgumentException("smallestKey must be <= largestKey");
        }
        if (smallestRevision > largestRevision) {
            throw new IllegalArgumentException("smallestRevision must be <= largestRevision");
        }
    }

    public boolean overlaps(byte[] startKey, byte[] endKey) {
        boolean afterStart = startKey == null || Arrays.compareUnsigned(largestKey, startKey) >= 0;
        boolean beforeEnd = endKey == null || Arrays.compareUnsigned(smallestKey, endKey) <= 0;
        return afterStart && beforeEnd;
    }

    public int serializedSize() {
        return 8 + 4 + 4 + smallestKey.length + 4 + largestKey.length + 8 + 8 + 8 + 8;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(id);
        buffer.putInt(level);
        buffer.putInt(smallestKey.length);
        buffer.put(smallestKey);
        buffer.putInt(largestKey.length);
        buffer.put(largestKey);
        buffer.putLong(smallestRevision);
        buffer.putLong(largestRevision);
        buffer.putLong(fileSizeBytes);
        buffer.putLong(entryCount);
    }

    public static SSTableInfo readFrom(ByteBuffer buffer) {
        long id = buffer.getLong();
        int level = buffer.getInt();

        int smallestKeySize = buffer.getInt();
        byte[] smallestKey = new byte[smallestKeySize];
        buffer.get(smallestKey);

        int largestKeySize = buffer.getInt();
        byte[] largestKey = new byte[largestKeySize];
        buffer.get(largestKey);

        long smallestRevision = buffer.getLong();
        long largestRevision = buffer.getLong();

        long fileSizeBytes = buffer.getLong();
        long entryCount = buffer.getLong();

        return new SSTableInfo(id, level, smallestKey, largestKey, smallestRevision, largestRevision, fileSizeBytes, entryCount);
    }
}
