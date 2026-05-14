package io.partdb.consensus;

public record ReadBarrier(long index, long term) {
    public ReadBarrier {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative");
        }
    }
}
