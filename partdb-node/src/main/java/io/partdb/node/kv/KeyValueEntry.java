package io.partdb.node.kv;

import io.partdb.bytes.Bytes;
import io.partdb.node.lease.LeaseId;

import java.util.Objects;
import java.util.Optional;

public record KeyValueEntry(
    Bytes key,
    Bytes value,
    long modRevision,
    Optional<LeaseId> leaseId
) {
    public KeyValueEntry {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
        leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        if (modRevision <= 0) {
            throw new IllegalArgumentException("modRevision must be positive");
        }
    }
}
