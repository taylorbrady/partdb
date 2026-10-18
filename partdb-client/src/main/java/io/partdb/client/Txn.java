package io.partdb.client;

import java.util.List;
import java.util.Objects;

public record Txn(List<Condition> conditions, List<Mutation> mutations) {
    public Txn {
        conditions = List.copyOf(Objects.requireNonNull(conditions, "conditions must not be null"));
        mutations = List.copyOf(Objects.requireNonNull(mutations, "mutations must not be null"));
        if (mutations.isEmpty()) {
            throw new IllegalArgumentException("a transaction needs at least one mutation");
        }
    }

    public static Txn of(List<Mutation> mutations) {
        return new Txn(List.of(), mutations);
    }

    public static Txn when(List<Condition> conditions, List<Mutation> mutations) {
        return new Txn(conditions, mutations);
    }
}
