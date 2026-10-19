package io.partdb.storage;

record TableMetadata(long id, Slice smallest, Slice largest, long sizeBytes, long tombstones) {

    TableMetadata {
        if (id <= 0) {
            throw new IllegalArgumentException("id must be positive");
        }
        if (smallest.compareTo(largest) > 0) {
            throw new IllegalArgumentException("smallest must be <= largest");
        }
        if (sizeBytes < 0 || tombstones < 0) {
            throw new IllegalArgumentException("sizeBytes and tombstones must not be negative");
        }
    }

    boolean overlaps(Slice start, Slice end) {
        return largest.compareTo(start) >= 0 && smallest.compareTo(end) <= 0;
    }

    boolean contains(Slice key) {
        return key.compareTo(smallest) >= 0 && key.compareTo(largest) <= 0;
    }
}
