package io.partdb.storage.sstable;

public record BlockHandle(long offset, int size) {

    public BlockHandle {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive");
        }
    }
}
