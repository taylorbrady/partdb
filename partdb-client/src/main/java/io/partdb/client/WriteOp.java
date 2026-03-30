package io.partdb.client;

public sealed interface WriteOp permits WriteOp.Put, WriteOp.Delete {

    record Put(byte[] key, byte[] value, long leaseId) implements WriteOp {
        public Put {
            key = key.clone();
            value = value.clone();
        }

        public Put(byte[] key, byte[] value) {
            this(key, value, 0);
        }

        @Override
        public byte[] key() {
            return key.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }
    }

    record Delete(byte[] key) implements WriteOp {
        public Delete {
            key = key.clone();
        }

        @Override
        public byte[] key() {
            return key.clone();
        }
    }
}
