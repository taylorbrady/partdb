package io.partdb.storage;

record Bounds(Slice start, Slice end) {

    static final Bounds ALL = new Bounds(null, null);

    static Bounds of(KeyRange range) {
        return new Bounds(range.start().isEmpty() ? null : Slice.of(range.start()), range.end().map(Slice::of).orElse(null));
    }

    boolean afterStart(Slice key) {
        return start == null || key.compareTo(start) >= 0;
    }

    boolean beforeEnd(Slice key) {
        return end == null || key.compareTo(end) < 0;
    }

    boolean overlaps(Slice smallest, Slice largest) {
        return afterStart(largest) && beforeEnd(smallest);
    }
}
