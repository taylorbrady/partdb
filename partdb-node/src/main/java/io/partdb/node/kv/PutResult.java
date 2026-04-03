package io.partdb.node.kv;

public record PutResult(long modRevision) {
    public PutResult {
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
