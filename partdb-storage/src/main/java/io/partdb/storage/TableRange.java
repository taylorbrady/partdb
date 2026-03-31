package io.partdb.storage;

import java.util.List;
import java.util.Objects;

record TableRange(Slice start, Slice end) {

    TableRange {
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
        if (start.compareTo(end) > 0) {
            throw new IllegalArgumentException("start must be <= end");
        }
    }

    boolean overlaps(TableRange other) {
        return end.compareTo(other.start) >= 0 && other.end.compareTo(start) >= 0;
    }

    static TableRange from(List<SSTableMetadata> sstables) {
        if (sstables.isEmpty()) {
            throw new IllegalArgumentException("sstables cannot be empty");
        }

        Slice min = null;
        Slice max = null;

        for (SSTableMetadata sst : sstables) {
            Slice smallest = sst.smallestKey();
            Slice largest = sst.largestKey();

            if (min == null || smallest.compareTo(min) < 0) {
                min = smallest;
            }
            if (max == null || largest.compareTo(max) > 0) {
                max = largest;
            }
        }

        return new TableRange(min, max);
    }
}
