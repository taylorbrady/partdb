package io.partdb.storage;

record TableMetadata(long id, int level, Slice smallest, Slice largest, long sizeBytes) {

    TableMetadata {
        if (id <= 0) {
            throw new IllegalArgumentException("id must be positive");
        }
        if (level < 0) {
            throw new IllegalArgumentException("level must not be negative");
        }
        if (smallest.compareTo(largest) > 0) {
            throw new IllegalArgumentException("smallest must be <= largest");
        }
    }

    boolean overlaps(Slice start, Slice end) {
        return largest.compareTo(start) >= 0 && smallest.compareTo(end) <= 0;
    }

    boolean overlaps(TableMetadata other) {
        return overlaps(other.smallest, other.largest);
    }

    boolean contains(Slice key) {
        return key.compareTo(smallest) >= 0 && key.compareTo(largest) <= 0;
    }
}
