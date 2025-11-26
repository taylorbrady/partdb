package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;

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
        if (startKey != null && largestKey.compareTo(startKey) < 0) {
            return false;
        }
        if (endKey != null && smallestKey.compareTo(endKey) >= 0) {
            return false;
        }
        return true;
    }
}
