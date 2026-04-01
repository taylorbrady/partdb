package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface Mutation permits Mutation.Put, Mutation.Delete {

    Bytes key();

    static Put put(Bytes key, Bytes value) {
        return new Put(key, value);
    }

    static Delete delete(Bytes key) {
        return new Delete(key);
    }

    record Put(Bytes key, Bytes value) implements Mutation {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }
    }

    record Delete(Bytes key) implements Mutation {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }
}
