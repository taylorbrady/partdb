package io.partdb.node.state;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.CommittedCommand;
import io.partdb.consensus.StateMachineResult;
import io.partdb.consensus.ReplicatedStateMachine;
import io.partdb.consensus.StoredSnapshot;
import io.partdb.node.internal.command.PartDbCommand;
import io.partdb.node.internal.command.PartDbCommandCodec;
import io.partdb.node.internal.command.PartDbCommandResult;
import io.partdb.node.internal.command.PartDbCommandResultCodec;
import io.partdb.node.kv.WriteBatchOperation;
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
        PartDbCommand command = PartDbCommandCodec.decode(committed.payload());
        long index = committed.index();

        StateMachineResult result = switch (command) {
            case PartDbCommand.Put(var key, var value) -> {
                store.apply(new Revision(index), Mutation.put(key, value));
                yield applied(new PartDbCommandResult.PutApplied(index));
            }
            case PartDbCommand.Delete(var key) -> {
                store.apply(new Revision(index), Mutation.delete(key));
                yield applied(new PartDbCommandResult.DeleteApplied(index));
            }
            case PartDbCommand.BatchWrite(var batch) -> {
                WriteBatch.Builder storageBatch = WriteBatch.builder();

                for (var operation : batch.operations()) {
                    switch (operation) {
                        case WriteBatchOperation.Put(var key, var value) -> storageBatch.put(key, value);
                        case WriteBatchOperation.Delete(var key) -> storageBatch.delete(key);
                    }
                }

                store.apply(new Revision(index), storageBatch.build());

                yield applied(new PartDbCommandResult.BatchWriteApplied(index));
            }
        };

        lastApplied = index;
        return result;
    }

    public Optional<Bytes> getLocal(Bytes key) {
        return getLocalValue(key).map(LocalValue::value);
    }

    public Optional<LocalValue> getLocalValue(Bytes key) {
        Optional<ValueRecord> raw = store.get(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LocalValue(
            raw.get().value(),
            raw.get().modRevision().value()
        ));
    }

    public Stream<KvEntry> scanLocal(KeyRange range) {
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

    public record LocalValue(Bytes value, long modRevision) {}

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

    private static StateMachineResult applied(PartDbCommandResult.AppliedCommandResult result) {
        return new StateMachineResult.Applied(PartDbCommandResultCodec.encode(result));
    }
}
