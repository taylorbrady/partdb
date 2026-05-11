package io.partdb.client;

import io.partdb.bytes.Bytes;

public sealed interface WriteOp permits WriteOp.Put, WriteOp.Delete {

    record Put(Bytes key, Bytes value) implements WriteOp {
        public Put {
            if (key == null) {
                throw new IllegalArgumentException("key must not be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("value must not be null");
            }
        }
    }

    record Delete(Bytes key) implements WriteOp {
        public Delete {
            if (key == null) {
                throw new IllegalArgumentException("key must not be null");
            }
        }
    }
}
