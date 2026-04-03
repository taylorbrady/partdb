package io.partdb.node.kv;

import io.partdb.bytes.Bytes;
import io.partdb.node.lease.LeaseId;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record WriteBatch(List<WriteBatchOperation> operations) {
    public WriteBatch {
        operations = List.copyOf(Objects.requireNonNull(operations, "operations must not be null"));
        if (operations.isEmpty()) {
            throw new IllegalArgumentException("operations must not be empty");
        }
        Set<Bytes> keys = new LinkedHashSet<>(operations.size());
        for (WriteBatchOperation operation : operations) {
            Objects.requireNonNull(operation, "operation must not be null");
            if (!keys.add(operation.key())) {
                throw new IllegalArgumentException("write batch contains duplicate key");
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<WriteBatchOperation> operations = new ArrayList<>();

        public Builder put(Bytes key, Bytes value) {
            operations.add(WriteBatchOperation.Put.of(key, value));
            return this;
        }

        public Builder put(Bytes key, Bytes value, LeaseId leaseId) {
            operations.add(WriteBatchOperation.Put.of(key, value, leaseId));
            return this;
        }

        public Builder delete(Bytes key) {
            operations.add(new WriteBatchOperation.Delete(key));
            return this;
        }

        public WriteBatch build() {
            return new WriteBatch(operations);
        }
    }
}
