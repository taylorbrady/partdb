package io.partdb.node.kv;

public record WriteBatchResult(long modRevision) {
    public WriteBatchResult {
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
