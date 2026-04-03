package io.partdb.node.kv;

public record DeleteResult(long modRevision) {
    public DeleteResult {
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
