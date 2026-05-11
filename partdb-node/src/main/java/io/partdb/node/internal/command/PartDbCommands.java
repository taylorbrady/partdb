package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.DeleteResult;
import io.partdb.node.kv.PutResult;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteBatchResult;

import java.util.Objects;

public final class PartDbCommands {
    private PartDbCommands() {
    }

    public static ReplicatedCommand<PutResult> put(Bytes key, Bytes value) {
        return new Put(key, value);
    }

    public static ReplicatedCommand<DeleteResult> delete(Bytes key) {
        return new Delete(key);
    }

    public static ReplicatedCommand<WriteBatchResult> writeBatch(WriteBatch batch) {
        return new BatchWrite(batch);
    }

    private record Put(Bytes key, Bytes value) implements ReplicatedCommand<PutResult> {
        private Put {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.Put(key, value);
        }

        @Override
        public PutResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.PutApplied(long modRevision) -> new PutResult(modRevision);
                default -> throw unexpected("put", result);
            };
        }
    }

    private record Delete(Bytes key) implements ReplicatedCommand<DeleteResult> {
        private Delete {
            key = Objects.requireNonNull(key, "key must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.Delete(key);
        }

        @Override
        public DeleteResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.DeleteApplied(long modRevision) -> new DeleteResult(modRevision);
                default -> throw unexpected("delete", result);
            };
        }
    }

    private record BatchWrite(WriteBatch batch) implements ReplicatedCommand<WriteBatchResult> {
        private BatchWrite {
            batch = Objects.requireNonNull(batch, "batch must not be null");
        }

        @Override
        public PartDbCommand payload() {
            return new PartDbCommand.BatchWrite(batch);
        }

        @Override
        public WriteBatchResult mapResult(PartDbCommandResult result) {
            return switch (result) {
                case PartDbCommandResult.BatchWriteApplied(long modRevision) -> new WriteBatchResult(modRevision);
                default -> throw unexpected("writeBatch", result);
            };
        }
    }

    private static IllegalStateException unexpected(String operation, PartDbCommandResult result) {
        return new IllegalStateException("Unexpected command result for " + operation + ": " + result);
    }
}
