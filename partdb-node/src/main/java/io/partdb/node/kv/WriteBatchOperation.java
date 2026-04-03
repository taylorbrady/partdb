package io.partdb.node.kv;

import io.partdb.bytes.Bytes;
import io.partdb.node.lease.LeaseId;

import java.util.Objects;
import java.util.Optional;

public sealed interface WriteBatchOperation permits WriteBatchOperation.Put, WriteBatchOperation.Delete {
    Bytes key();

    record Put(Bytes key, Bytes value, Optional<LeaseId> leaseId) implements WriteBatchOperation {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
            leaseId = Objects.requireNonNull(leaseId, "leaseId must not be null");
        }

        public static Put of(Bytes key, Bytes value) {
            return new Put(key, value, Optional.empty());
        }

        public static Put of(Bytes key, Bytes value, LeaseId leaseId) {
            return new Put(key, value, Optional.of(Objects.requireNonNull(leaseId, "leaseId must not be null")));
        }
    }

    record Delete(Bytes key) implements WriteBatchOperation {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }
}
