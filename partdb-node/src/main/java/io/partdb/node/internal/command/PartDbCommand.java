package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;

import java.util.Objects;

public sealed interface PartDbCommand
    permits PartDbCommand.Put,
            PartDbCommand.Delete,
            PartDbCommand.BatchWrite {

    record Put(Bytes key, Bytes value) implements PartDbCommand {
        public Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }
    }

    record Delete(Bytes key) implements PartDbCommand {
        public Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }

    record BatchWrite(WriteBatch batch) implements PartDbCommand {
        public BatchWrite {
            batch = Objects.requireNonNull(batch, "batch must not be null");
        }
    }

}
