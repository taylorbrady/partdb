package io.partdb.node.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;

import java.util.Objects;

public sealed interface KvCommand permits KvCommand.Put, KvCommand.Delete, KvCommand.BatchWrite {
    record Put(Bytes key, Bytes value) implements KvCommand {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }
    }

    record Delete(Bytes key) implements KvCommand {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }

    record BatchWrite(WriteBatch batch) implements KvCommand {
        public BatchWrite {
            batch = Objects.requireNonNull(batch, "batch must not be null");
        }
    }
}
