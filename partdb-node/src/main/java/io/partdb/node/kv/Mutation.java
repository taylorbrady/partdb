package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface Mutation {
    Bytes key();

    static Put put(Bytes key, Bytes value) {
        return new Put(key, value);
    }

    static Delete delete(Bytes key) {
        return new Delete(key);
    }

    record Put(Bytes key, Bytes value) implements Mutation {
        public Put {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(value, "value must not be null");
        }
    }

    record Delete(Bytes key) implements Mutation {
        public Delete {
            Objects.requireNonNull(key, "key must not be null");
        }
    }
}
