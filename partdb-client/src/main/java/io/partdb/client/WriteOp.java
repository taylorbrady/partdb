package io.partdb.client;

import io.partdb.common.ByteArray;

public sealed interface WriteOp permits WriteOp.Put, WriteOp.Delete {

    record Put(ByteArray key, ByteArray value, long leaseId) implements WriteOp {
        public Put(ByteArray key, ByteArray value) {
            this(key, value, 0);
        }
    }

    record Delete(ByteArray key) implements WriteOp {}
}
