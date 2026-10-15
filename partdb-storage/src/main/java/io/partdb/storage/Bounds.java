package io.partdb.storage;

record Bounds(Slice start, Slice end) {

    static final Bounds ALL = new Bounds(null, null);

    static Bounds of(KeyRange range) {
        return switch (range) {
            case KeyRange.All _ -> ALL;
            case KeyRange.From(var start) -> new Bounds(Slice.of(start), null);
            case KeyRange.Until(var end) -> new Bounds(null, Slice.of(end));
            case KeyRange.Between(var start, var end) -> new Bounds(Slice.of(start), Slice.of(end));
        };
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
