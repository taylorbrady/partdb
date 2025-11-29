package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;

import java.nio.ByteBuffer;
import java.util.Objects;

public record SSTableMetadata(
    long id,
    int level,
    ByteArray smallestKey,
    ByteArray largestKey,
    long fileSizeBytes,
    long entryCount
) {

    public SSTableMetadata {
        Objects.requireNonNull(smallestKey, "smallestKey cannot be null");
        Objects.requireNonNull(largestKey, "largestKey cannot be null");

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
    }

    public boolean overlaps(ByteArray startKey, ByteArray endKey) {
        boolean afterStart = startKey == null || largestKey.compareTo(startKey) >= 0;
        boolean beforeEnd = endKey == null || smallestKey.compareTo(endKey) < 0;
        return afterStart && beforeEnd;
    }

    public int serializedSize() {
        return 8 + 4 + 4 + smallestKey.size() + 4 + largestKey.size() + 8 + 8;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(id);
        buffer.putInt(level);
        buffer.putInt(smallestKey.size());
        buffer.put(smallestKey.toByteArray());
        buffer.putInt(largestKey.size());
        buffer.put(largestKey.toByteArray());
        buffer.putLong(fileSizeBytes);
        buffer.putLong(entryCount);
    }

    public static SSTableMetadata readFrom(ByteBuffer buffer) {
        long id = buffer.getLong();
        int level = buffer.getInt();

        int smallestKeySize = buffer.getInt();
        byte[] smallestKeyBytes = new byte[smallestKeySize];
        buffer.get(smallestKeyBytes);
        ByteArray smallestKey = ByteArray.wrap(smallestKeyBytes);

        int largestKeySize = buffer.getInt();
        byte[] largestKeyBytes = new byte[largestKeySize];
        buffer.get(largestKeyBytes);
        ByteArray largestKey = ByteArray.wrap(largestKeyBytes);

        long fileSizeBytes = buffer.getLong();
        long entryCount = buffer.getLong();

        return new SSTableMetadata(id, level, smallestKey, largestKey, fileSizeBytes, entryCount);
    }
}
