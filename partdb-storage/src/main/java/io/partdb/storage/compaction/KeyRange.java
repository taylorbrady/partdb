package io.partdb.storage.compaction;

import io.partdb.common.Slice;
import io.partdb.storage.sstable.SSTableDescriptor;

import java.util.List;
import java.util.Objects;

public record KeyRange(Slice start, Slice end) {

    public KeyRange {
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
        if (start.compareTo(end) > 0) {
            throw new IllegalArgumentException("start must be <= end");
        }
    }

    public boolean overlaps(KeyRange other) {
        return end.compareTo(other.start) >= 0 && other.end.compareTo(start) >= 0;
    }

    public static KeyRange from(List<SSTableDescriptor> sstables) {
        if (sstables.isEmpty()) {
            throw new IllegalArgumentException("sstables cannot be empty");
        }

        Slice min = null;
        Slice max = null;

        for (SSTableDescriptor sst : sstables) {
            Slice smallest = sst.smallestKey();
            Slice largest = sst.largestKey();

            if (min == null || smallest.compareTo(min) < 0) {
                min = smallest;
            }
            if (max == null || largest.compareTo(max) > 0) {
                max = largest;
            }
        }

        return new KeyRange(min, max);
    }
}
