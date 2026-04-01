package io.partdb.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class WriteBatch {

    private final List<Mutation> mutations;

    private WriteBatch(List<Mutation> mutations) {
        this.mutations = List.copyOf(Objects.requireNonNull(mutations, "mutations must not be null"));
    }

    public static WriteBatch of(Mutation... mutations) {
        Objects.requireNonNull(mutations, "mutations must not be null");
        return new WriteBatch(Arrays.asList(mutations.clone()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Mutation> mutations() {
        return mutations;
    }

    public boolean isEmpty() {
        return mutations.isEmpty();
    }

    public static final class Builder {
        private final List<Mutation> mutations = new ArrayList<>();

        private Builder() {
        }

        public Builder add(Mutation mutation) {
            mutations.add(Objects.requireNonNull(mutation, "mutation must not be null"));
            return this;
        }

        public Builder put(io.partdb.bytes.Bytes key, io.partdb.bytes.Bytes value) {
            return add(Mutation.put(key, value));
        }

        public Builder delete(io.partdb.bytes.Bytes key) {
            return add(Mutation.delete(key));
        }

        public WriteBatch build() {
            return new WriteBatch(mutations);
        }
    }
}
