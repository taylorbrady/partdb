package io.partdb.node.lease;

import java.time.Duration;
import java.util.Objects;

public record LeaseKeepAliveResult(LeaseId leaseId, Duration ttl, long modRevision) {
    public LeaseKeepAliveResult {
        leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        ttl = Objects.requireNonNull(ttl, "ttl must not be null");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
