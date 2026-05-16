package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record WriteBatch(List<WriteOperation> operations) {
    public WriteBatch {
        operations = List.copyOf(Objects.requireNonNull(operations, "operations must not be null"));
        if (operations.isEmpty()) {
            throw new IllegalArgumentException("operations must not be empty");
        }
        Set<Bytes> keys = new LinkedHashSet<>(operations.size());
        for (WriteOperation operation : operations) {
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
        private final List<WriteOperation> operations = new ArrayList<>();

        public Builder put(Bytes key, Bytes value) {
            operations.add(new WriteOperation.Put(key, value));
            return this;
        }

        public Builder delete(Bytes key) {
            operations.add(new WriteOperation.Delete(key));
            return this;
        }

        public WriteBatch build() {
            return new WriteBatch(operations);
        }
    }
}
