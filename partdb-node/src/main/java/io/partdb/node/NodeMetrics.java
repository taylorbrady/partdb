package io.partdb.node;

import java.util.Objects;

public record NodeMetrics(long proposals, long failedProposals, StorageMetrics storage) {
    public NodeMetrics {
        Objects.requireNonNull(storage, "storage must not be null");
    }
}
