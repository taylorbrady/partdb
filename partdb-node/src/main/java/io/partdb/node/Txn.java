package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.storage.Mutation;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public record Txn(List<Condition> conditions, List<Mutation> mutations) {
    public Txn {
        conditions = List.copyOf(Objects.requireNonNull(conditions, "conditions must not be null"));
        mutations = List.copyOf(Objects.requireNonNull(mutations, "mutations must not be null"));
        if (mutations.isEmpty()) {
            throw new IllegalArgumentException("a transaction needs at least one mutation");
        }
        var keys = new HashSet<Bytes>();
        for (Mutation mutation : mutations) {
            if (!keys.add(mutation.key())) {
                throw new IllegalArgumentException("a transaction mutates key " + mutation.key() + " more than once");
            }
        }
    }

    public static Txn of(List<Mutation> mutations) {
        return new Txn(List.of(), mutations);
    }

    public static Txn when(List<Condition> conditions, List<Mutation> mutations) {
        return new Txn(conditions, mutations);
    }
}
