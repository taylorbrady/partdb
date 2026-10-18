package io.partdb.node;

import io.partdb.storage.StoreStats;

import java.util.Objects;

public record NodeMetrics(long proposals, long failedProposals, StoreStats storage) {
    public NodeMetrics {
        Objects.requireNonNull(storage, "storage must not be null");
    }
}
