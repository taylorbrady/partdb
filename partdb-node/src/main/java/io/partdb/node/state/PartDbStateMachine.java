package io.partdb.node.state;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.CommittedCommand;
import io.partdb.consensus.StateMachineResult;
import io.partdb.consensus.ReplicatedStateMachine;
import io.partdb.consensus.StoredSnapshot;
import io.partdb.node.command.KvCommand;
import io.partdb.node.command.KvCommandCodec;
import io.partdb.node.command.KvCommandResult;
import io.partdb.node.command.KvCommandResultCodec;
import io.partdb.node.kv.Condition;
import io.partdb.node.kv.WriteOperation;
import io.partdb.storage.EntryRecord;
import io.partdb.storage.KeyRange;
import io.partdb.storage.Mutation;
import io.partdb.storage.Revision;
import io.partdb.storage.Scan;
import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageCheckpoint;
import io.partdb.storage.StorageEngine;
import io.partdb.storage.StorageStats;
import io.partdb.storage.ValueRecord;
import io.partdb.storage.WriteBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class PartDbStateMachine implements ReplicatedStateMachine, AutoCloseable {

    private final StorageEngine store;
    private volatile long lastApplied;

    private PartDbStateMachine(StorageEngine store) {
        this.store = store;
        this.lastApplied = 0;
    }

    public static PartDbStateMachine open(Path dataDirectory, StorageOptions options) {
        StorageEngine store = StorageEngine.open(dataDirectory, options);
        return new PartDbStateMachine(store);
    }

    @Override
    public StateMachineResult apply(CommittedCommand committed) {
        KvCommand command = KvCommandCodec.decode(committed.payload());
        long index = committed.index();

        StateMachineResult result = switch (command) {
            case KvCommand.Put(var key, var value) -> {
                store.apply(new Revision(index), Mutation.put(key, value));
                yield applied(index);
            }
            case KvCommand.Delete(var key) -> {
                store.apply(new Revision(index), Mutation.delete(key));
                yield applied(index);
            }
            case KvCommand.BatchWrite(var batch) -> {
                store.apply(new Revision(index), toStorageBatch(batch.operations()));

                yield applied(index);
            }
            case KvCommand.CompareAndWrite(var transaction) -> {
                if (!conditionsMatch(transaction.conditions())) {
                    yield conditionFailed();
                }
                store.apply(new Revision(index), toStorageBatch(transaction.operations()));
                yield applied(index);
            }
        };

        lastApplied = index;
        return result;
    }

    public Optional<Bytes> get(Bytes key) {
        return getValue(key).map(StoredValue::value);
    }

    public Optional<StoredValue> getValue(Bytes key) {
        Optional<ValueRecord> raw = store.get(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new StoredValue(
            raw.get().value(),
            raw.get().modRevision().value()
        ));
    }

    public Stream<KvEntry> scan(KeyRange range) {
        Scan cursor = store.scan(range);
        Iterator<EntryRecord> entries = cursor.iterator();

        Iterator<KvEntry> iterator = new Iterator<>() {
            private KvEntry next = advance();

            private KvEntry advance() {
                while (entries.hasNext()) {
                    EntryRecord entry = entries.next();
                    return new KvEntry(entry.key(), entry.value(), entry.modRevision().value());
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public KvEntry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                KvEntry current = next;
                next = advance();
                return current;
            }
        };

        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL),
            false
        ).onClose(cursor::close);
    }

    public record KvEntry(Bytes key, Bytes value, long version) {}

    @Override
    public StoredSnapshot snapshot() {
        try {
            byte[] storageData = store.checkpoint().bytes().toByteArray();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(12);
            header.putLong(lastApplied);
            header.putInt(storageData.length);
            baos.write(header.array());
            baos.write(storageData);

            return new StoredSnapshot(lastApplied, 0, Bytes.copyOf(baos.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restore(StoredSnapshot snapshot) {
        ByteBuffer buffer = ByteBuffer.wrap(snapshot.data().toByteArray());
        buffer.getLong();
        int storageLen = buffer.getInt();

        byte[] storageData = new byte[storageLen];
        buffer.get(storageData);

        store.restore(new StorageCheckpoint(Bytes.copyOf(storageData)));

        lastApplied = snapshot.index();
    }

    public long lastAppliedIndex() {
        return lastApplied;
    }

    public StorageStats storageStats() {
        return store.stats();
    }

    @Override
    public void close() {
        store.close();
    }

    private static StateMachineResult applied(long revision) {
        return new StateMachineResult.Applied(KvCommandResultCodec.encode(new KvCommandResult.Applied(revision)));
    }

    private static StateMachineResult conditionFailed() {
        return new StateMachineResult.Applied(KvCommandResultCodec.encode(new KvCommandResult.ConditionFailed()));
    }

    private boolean conditionsMatch(List<Condition> conditions) {
        for (Condition condition : conditions) {
            if (!conditionMatches(condition)) {
                return false;
            }
        }
        return true;
    }

    private boolean conditionMatches(Condition condition) {
        Optional<ValueRecord> value = store.get(condition.key());
        return switch (condition) {
            case Condition.Exists _ -> value.isPresent();
            case Condition.Missing _ -> value.isEmpty();
            case Condition.ValueEquals(var ignored, var expected) -> value
                .map(ValueRecord::value)
                .filter(expected::equals)
                .isPresent();
            case Condition.RevisionEquals(var ignored, long expectedRevision) -> value
                .map(ValueRecord::modRevision)
                .map(Revision::value)
                .filter(revision -> revision == expectedRevision)
                .isPresent();
        };
    }

    private static WriteBatch toStorageBatch(List<WriteOperation> operations) {
        WriteBatch.Builder storageBatch = WriteBatch.builder();
        for (var operation : operations) {
            switch (operation) {
                case WriteOperation.Put(var key, var value) -> storageBatch.put(key, value);
                case WriteOperation.Delete(var key) -> storageBatch.delete(key);
            }
        }
        return storageBatch.build();
    }
}
