package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface WriteBatchOperation permits WriteBatchOperation.Put, WriteBatchOperation.Delete {
    Bytes key();

    record Put(Bytes key, Bytes value) implements WriteBatchOperation {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }

        public static Put of(Bytes key, Bytes value) {
            return new Put(key, value);
        }
    }

    record Delete(Bytes key) implements WriteBatchOperation {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }
}
