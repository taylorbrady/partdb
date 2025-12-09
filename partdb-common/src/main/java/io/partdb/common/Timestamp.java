package io.partdb.common;

public record Timestamp(long value) implements Comparable<Timestamp> {

    public static final Timestamp ZERO = new Timestamp(0);
    public static final Timestamp MAX = new Timestamp(Long.MAX_VALUE);

    public long wallTime() {
        return value >>> 16;
    }

    public int logical() {
        return (int) (value & 0xFFFF);
    }

    public static Timestamp of(long wallTimeMs, int logical) {
        return new Timestamp((wallTimeMs << 16) | (logical & 0xFFFF));
    }

    @Override
    public int compareTo(Timestamp other) {
        return Long.compareUnsigned(this.value, other.value);
    }
}
