package io.partdb.storage.internal;

import io.partdb.storage.*;

final class ScanBounds {

    private final Slice startInclusive;
    private final Slice endExclusive;

    private ScanBounds(Slice startInclusive, Slice endExclusive) {
        if (startInclusive != null && endExclusive != null && startInclusive.compareTo(endExclusive) > 0) {
            throw new IllegalArgumentException("startInclusive must be <= endExclusive");
        }
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    static ScanBounds all() {
        return new ScanBounds(null, null);
    }

    static ScanBounds from(Slice startInclusive) {
        return new ScanBounds(startInclusive, null);
    }

    static ScanBounds until(Slice endExclusive) {
        return new ScanBounds(null, endExclusive);
    }

    static ScanBounds between(Slice startInclusive, Slice endExclusive) {
        return new ScanBounds(startInclusive, endExclusive);
    }

    Slice startInclusive() {
        return startInclusive;
    }

    Slice endExclusive() {
        return endExclusive;
    }

    boolean isAll() {
        return startInclusive == null && endExclusive == null;
    }

    boolean includes(Slice key) {
        boolean afterStart = startInclusive == null || key.compareTo(startInclusive) >= 0;
        boolean beforeEnd = endExclusive == null || key.compareTo(endExclusive) < 0;
        return afterStart && beforeEnd;
    }

    boolean overlaps(Slice smallestKey, Slice largestKey) {
        boolean afterStart = startInclusive == null || largestKey.compareTo(startInclusive) >= 0;
        boolean beforeEnd = endExclusive == null || smallestKey.compareTo(endExclusive) < 0;
        return afterStart && beforeEnd;
    }
}
