package io.partdb.storage;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

record SSTableMetadata(
    long id,
    int level,
    Slice smallestKey,
    Slice largestKey,
    long smallestRevision,
    long largestRevision,
    long fileSizeBytes,
    long entryCount
) {

    public SSTableMetadata {
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

    public boolean overlaps(ScanBounds bounds) {
        return bounds.overlaps(smallestKey, largestKey);
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

    public static SSTableMetadata readFrom(ByteBuffer buffer) {
        try {
            long id = buffer.getLong();
            int level = buffer.getInt();

            int smallestKeySize = buffer.getInt();
            byte[] smallestKeyBytes = readSizedBytes(buffer, smallestKeySize, "smallest key");

            int largestKeySize = buffer.getInt();
            byte[] largestKeyBytes = readSizedBytes(buffer, largestKeySize, "largest key");

            long smallestRevision = buffer.getLong();
            long largestRevision = buffer.getLong();
            long fileSizeBytes = buffer.getLong();
            long entryCount = buffer.getLong();

            return new SSTableMetadata(
                id,
                level,
                Slice.copyOf(smallestKeyBytes),
                Slice.copyOf(largestKeyBytes),
                smallestRevision,
                largestRevision,
                fileSizeBytes,
                entryCount
            );
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new StorageException.Corruption("Malformed SSTable metadata", e);
        }
    }

    private static byte[] readSizedBytes(ByteBuffer buffer, int size, String fieldName) {
        if (size < 0) {
            throw new StorageException.Corruption("Negative SSTable metadata " + fieldName + " length");
        }
        if (buffer.remaining() < size) {
            throw new StorageException.Corruption("Truncated SSTable metadata " + fieldName);
        }

        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return bytes;
    }
}
