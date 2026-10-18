package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.storage.Mutation;

public record KvLimits(int maxKeyBytes, int maxValueBytes, int maxMutations, int maxConditions, int maxTxnBytes) {
    public static final KvLimits DEFAULT = new KvLimits(4 << 10, 1 << 20, 1024, 1024, 4 << 20);

    public KvLimits {
        if (maxKeyBytes <= 0 || maxValueBytes <= 0 || maxMutations <= 0 || maxConditions <= 0 || maxTxnBytes <= 0) {
            throw new IllegalArgumentException("limits must be positive");
        }
    }

    Txn check(Txn txn) {
        if (txn.mutations().size() > maxMutations) {
            throw new IllegalArgumentException("a transaction may have at most " + maxMutations + " mutations");
        }
        if (txn.conditions().size() > maxConditions) {
            throw new IllegalArgumentException("a transaction may have at most " + maxConditions + " conditions");
        }
        long bytes = 0;
        for (Condition condition : txn.conditions()) {
            bytes += checkKey(condition.key()).size();
            if (condition instanceof Condition.ValueEquals(_, var value)) {
                bytes += checkValue(value);
            }
        }
        for (Mutation mutation : txn.mutations()) {
            bytes += checkKey(mutation.key()).size();
            if (mutation instanceof Mutation.Put(_, var value)) {
                bytes += checkValue(value);
            }
        }
        if (bytes > maxTxnBytes) {
            throw new IllegalArgumentException("a transaction may carry at most " + maxTxnBytes + " bytes of keys and values");
        }
        return txn;
    }

    Bytes checkKey(Bytes key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (key.size() > maxKeyBytes) {
            throw new IllegalArgumentException("key exceeds " + maxKeyBytes + " bytes");
        }
        return key;
    }

    private long checkValue(Bytes value) {
        if (value.size() > maxValueBytes) {
            throw new IllegalArgumentException("value exceeds " + maxValueBytes + " bytes");
        }
        return value.size();
    }
}
