package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record Transaction(List<Condition> conditions, List<WriteOperation> operations) {
    public Transaction {
        conditions = List.copyOf(Objects.requireNonNull(conditions, "conditions must not be null"));
        operations = List.copyOf(Objects.requireNonNull(operations, "operations must not be null"));
        if (operations.isEmpty()) {
            throw new IllegalArgumentException("operations must not be empty");
        }

        for (Condition condition : conditions) {
            Objects.requireNonNull(condition, "condition must not be null");
        }

        Set<Bytes> keys = new LinkedHashSet<>(operations.size());
        for (WriteOperation operation : operations) {
            Objects.requireNonNull(operation, "operation must not be null");
            if (!keys.add(operation.key())) {
                throw new IllegalArgumentException("transaction contains duplicate write key");
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public WriteBatch writeBatch() {
        return new WriteBatch(operations);
    }

    public static final class Builder {
        private final List<Condition> conditions = new ArrayList<>();
        private final List<WriteOperation> operations = new ArrayList<>();

        private Builder() {
        }

        public Builder require(Condition condition) {
            conditions.add(Objects.requireNonNull(condition, "condition must not be null"));
            return this;
        }

        public Builder put(Bytes key, Bytes value) {
            operations.add(new WriteOperation.Put(key, value));
            return this;
        }

        public Builder delete(Bytes key) {
            operations.add(new WriteOperation.Delete(key));
            return this;
        }

        public Transaction build() {
            return new Transaction(conditions, operations);
        }
    }
}
