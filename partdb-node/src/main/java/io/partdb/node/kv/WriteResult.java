package io.partdb.node.kv;

public record WriteResult(long revision) {
    public WriteResult {
        if (revision <= 0) {
            throw new IllegalArgumentException("revision must be positive");
        }
    }
}
