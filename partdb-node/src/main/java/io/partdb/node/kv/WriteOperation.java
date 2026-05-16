package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface WriteOperation permits WriteOperation.Put, WriteOperation.Delete {
    Bytes key();

    record Put(Bytes key, Bytes value) implements WriteOperation {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }
    }

    record Delete(Bytes key) implements WriteOperation {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }
}
