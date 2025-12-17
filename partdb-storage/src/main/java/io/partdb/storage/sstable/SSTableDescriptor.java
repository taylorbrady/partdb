package io.partdb.storage.sstable;

import io.partdb.common.Slice;

import java.nio.ByteBuffer;
import java.util.Objects;

public record SSTableDescriptor(
    long id,
    int level,
    Slice smallestKey,
    Slice largestKey,
    long smallestRevision,
    long largestRevision,
    long fileSizeBytes,
    long entryCount
) {

    public SSTableDescriptor {
        Objects.requireNonNull(smallestKey, "smallestKey");
        Objects.requireNonNull(largestKey, "largestKey");
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
        if (smallestRevision > largestRevision) {
            throw new IllegalArgumentException("smallestRevision must be <= largestRevision");
        }
    }

    public boolean overlaps(Slice startKey, Slice endKey) {
        boolean afterStart = startKey == null || largestKey.compareTo(startKey) >= 0;
        boolean beforeEnd = endKey == null || smallestKey.compareTo(endKey) <= 0;
        return afterStart && beforeEnd;
    }

    public boolean mightContain(Slice key) {
        return key.compareTo(smallestKey) >= 0 && key.compareTo(largestKey) <= 0;
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
        buffer.putLong(smallestRevision);
        buffer.putLong(largestRevision);
        buffer.putLong(fileSizeBytes);
        buffer.putLong(entryCount);
    }

    public static SSTableDescriptor readFrom(ByteBuffer buffer) {
        long id = buffer.getLong();
        int level = buffer.getInt();

        int smallestKeySize = buffer.getInt();
        byte[] smallestKeyBytes = new byte[smallestKeySize];
        buffer.get(smallestKeyBytes);

        int largestKeySize = buffer.getInt();
        byte[] largestKeyBytes = new byte[largestKeySize];
        buffer.get(largestKeyBytes);

        long smallestRevision = buffer.getLong();
        long largestRevision = buffer.getLong();
        long fileSizeBytes = buffer.getLong();
        long entryCount = buffer.getLong();

        return new SSTableDescriptor(
            id,
            level,
            Slice.of(smallestKeyBytes),
            Slice.of(largestKeyBytes),
            smallestRevision,
            largestRevision,
            fileSizeBytes,
            entryCount
        );
    }
}
