package io.partdb.storage;

public record Revision(long value) implements Comparable<Revision> {

    public Revision {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }
    }

    @Override
    public int compareTo(Revision other) {
        return Long.compare(value, other.value);
    }
}
