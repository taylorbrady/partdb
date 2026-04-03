package io.partdb.node.kv;

import io.partdb.bytes.Bytes;
import io.partdb.node.lease.LeaseId;

import java.util.Objects;
import java.util.Optional;

public record PutRequest(Bytes key, Bytes value, Optional<LeaseId> leaseId) {
    public PutRequest {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
        leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
    }

    public static PutRequest of(Bytes key, Bytes value) {
        return new PutRequest(key, value, Optional.empty());
    }

    public static PutRequest of(Bytes key, Bytes value, LeaseId leaseId) {
        return new PutRequest(key, value, Optional.of(Objects.requireNonNull(leaseId, "leaseId must not be null")));
    }
}
