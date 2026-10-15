package io.partdb.node.state;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.Condition;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValueEntry;
import io.partdb.node.kv.ScanCursor;
import io.partdb.node.kv.VersionedValue;
import io.partdb.node.kv.WriteOperation;
import io.partdb.raft.runtime.StateMachine;
import io.partdb.storage.EntryRecord;
import io.partdb.storage.Mutation;
import io.partdb.storage.MutationBatch;
import io.partdb.storage.Revision;
import io.partdb.storage.Scan;
import io.partdb.storage.StorageEngine;
import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageReadView;
import io.partdb.storage.StorageStats;
import io.partdb.storage.ValueRecord;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class PartDbStateMachine implements StateMachine, AutoCloseable {

    private final StorageEngine store;
    private volatile long lastApplied;

    private PartDbStateMachine(StorageEngine store) {
        this.store = store;
        this.lastApplied = store.appliedThrough().value();
    }

    public static PartDbStateMachine open(Path dataDirectory) {
        return new PartDbStateMachine(StorageEngine.open(dataDirectory, StorageOptions.defaults()));
    }

    @Override
    public long appliedIndex() {
        return lastApplied;
    }

    @Override
    public Bytes apply(long index, Bytes encoded) {
        if (index <= lastApplied) {
            return KvCommandResultCodec.encode(new KvCommandResult.Applied(index));
        }
        var revision = new Revision(index);
        KvCommandResult result = switch (KvCommandCodec.decode(encoded)) {
            case KvCommand.Put(var key, var value) -> {
                store.apply(revision, Mutation.put(key, value));
                yield new KvCommandResult.Applied(index);
            }
            case KvCommand.Delete(var key) -> {
                store.apply(revision, Mutation.delete(key));
                yield new KvCommandResult.Applied(index);
            }
            case KvCommand.BatchWrite(var batch) -> {
                store.apply(revision, toStorageBatch(batch.operations()));
                yield new KvCommandResult.Applied(index);
            }
            case KvCommand.CompareAndWrite(var transaction) -> {
                if (!conditionsMatch(transaction.conditions())) {
                    yield new KvCommandResult.ConditionFailed();
                }
                store.apply(revision, toStorageBatch(transaction.operations()));
                yield new KvCommandResult.Applied(index);
            }
        };
        lastApplied = index;
        return KvCommandResultCodec.encode(result);
    }

    @Override
    public void restore(long index, InputStream snapshot) {
        long revision = load(snapshot);
        if (revision != index) {
            throw new IllegalStateException("Snapshot " + index + " restored storage at revision " + revision);
        }
    }

    public long load(InputStream checkpoint) {
        store.restore(checkpoint);
        lastApplied = store.appliedThrough().value();
        return lastApplied;
    }

    public long checkpoint(OutputStream target) {
        return store.checkpoint(target).value();
    }

    public Optional<VersionedValue> get(Bytes key) {
        return store.get(key).map(record -> new VersionedValue(record.value(), record.modRevision().value()));
    }

    public ScanCursor<KeyValueEntry> scan(KeyRange range) {
        return new EntryCursor(store.scan(toStorageRange(range)));
    }

    public StorageStats storageStats() {
        return store.stats();
    }

    @Override
    public void close() {
        store.close();
    }

    private boolean conditionsMatch(List<Condition> conditions) {
        try (StorageReadView view = store.readView()) {
            return conditions.stream().allMatch(condition -> conditionMatches(view.get(condition.key()), condition));
        }
    }

    private static boolean conditionMatches(Optional<ValueRecord> value, Condition condition) {
        return switch (condition) {
            case Condition.Exists _ -> value.isPresent();
            case Condition.Missing _ -> value.isEmpty();
            case Condition.ValueEquals(var _, var expected) -> value.map(ValueRecord::value).filter(expected::equals).isPresent();
            case Condition.RevisionEquals(var _, long expected) -> value.filter(record -> record.modRevision().value() == expected).isPresent();
        };
    }

    private static MutationBatch toStorageBatch(List<WriteOperation> operations) {
        MutationBatch.Builder batch = MutationBatch.builder();
        for (var operation : operations) {
            switch (operation) {
                case WriteOperation.Put(var key, var value) -> batch.put(key, value);
                case WriteOperation.Delete(var key) -> batch.delete(key);
            }
        }
        return batch.build();
    }

    private static io.partdb.storage.KeyRange toStorageRange(KeyRange range) {
        var start = range.startInclusive();
        var end = range.endExclusive();
        if (start.isPresent() && end.isPresent()) {
            return io.partdb.storage.KeyRange.between(start.get(), end.get());
        }
        if (start.isPresent()) {
            return io.partdb.storage.KeyRange.from(start.get());
        }
        return end.map(io.partdb.storage.KeyRange::until).orElseGet(io.partdb.storage.KeyRange::all);
    }

    private static final class EntryCursor implements ScanCursor<KeyValueEntry> {
        private final Scan scan;
        private final Iterator<EntryRecord> entries;
        private boolean closed;

        private EntryCursor(Scan scan) {
            this.scan = scan;
            this.entries = scan.iterator();
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (entries.hasNext()) {
                return true;
            }
            close();
            return false;
        }

        @Override
        public KeyValueEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            EntryRecord entry = entries.next();
            return new KeyValueEntry(entry.key(), entry.value(), entry.modRevision().value());
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                scan.close();
            }
        }
    }
}
