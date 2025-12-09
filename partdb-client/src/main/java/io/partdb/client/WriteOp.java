package io.partdb.client;

public sealed interface WriteOp permits WriteOp.Put, WriteOp.Delete {

    record Put(byte[] key, byte[] value, long leaseId) implements WriteOp {
        public Put(byte[] key, byte[] value) {
            this(key, value, 0);
        }
    }

    record Delete(byte[] key) implements WriteOp {}
}
