package io.partdb.node.state;

import io.partdb.bytes.Bytes;
import io.partdb.node.StorageMetrics;
import io.partdb.node.kv.Condition;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValue;
import io.partdb.node.kv.Mutation;
import io.partdb.node.kv.Read;
import io.partdb.node.kv.Scan;
import io.partdb.node.kv.Txn;
import io.partdb.node.kv.TxnResult;
import io.partdb.raft.runtime.StateMachine;
import io.partdb.storage.Cursor;
import io.partdb.storage.Snapshot;
import io.partdb.storage.Store;
import io.partdb.storage.StoreOptions;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class KvStateMachine implements StateMachine<TxnResult>, AutoCloseable {

    private final Store store;

    private KvStateMachine(Store store) {
        this.store = store;
    }

    public static KvStateMachine open(Path directory) {
        return new KvStateMachine(Store.open(directory, StoreOptions.defaults()));
    }

    @Override
    public long appliedIndex() {
        return store.appliedIndex();
    }

    @Override
    public TxnResult apply(long index, Bytes command) {
        Txn txn = TxnCodec.decode(command);
        if (!matches(txn)) {
            store.apply(index, List.of());
            return new TxnResult.ConditionFailed();
        }
        store.apply(index, txn.mutations().stream().map(KvStateMachine::storageMutation).toList());
        return new TxnResult.Committed(index);
    }

    @Override
    public void skip(long index) {
        store.apply(index, List.of());
    }

    @Override
    public long snapshot(OutputStream target) {
        return store.checkpoint(target);
    }

    @Override
    public void restore(long index, InputStream snapshot) {
        long restored = store.restore(snapshot);
        if (restored != index) {
            throw new IllegalStateException("snapshot " + index + " restored storage at " + restored);
        }
    }

    public Read<Optional<KeyValue>> get(Bytes key) {
        try (Snapshot snapshot = store.snapshot()) {
            return new Read<>(snapshot.get(key).map(entry -> new KeyValue(key, entry.value(), entry.revision())), snapshot.index());
        }
    }

    public Scan scan(KeyRange range, int limit) {
        try (Snapshot snapshot = store.snapshot()) {
            return new StorageScan(snapshot.scan(storageRange(range)), limit, snapshot.index());
        }
    }

    public StorageMetrics metrics() {
        var stats = store.stats();
        return new StorageMetrics(
            stats.activeMemtableBytes(),
            stats.immutableMemtableCount(),
            stats.tableCount(),
            stats.tableBytes(),
            stats.activeCompactions(),
            stats.completedCompactions(),
            stats.failedCompactions(),
            stats.lastCompactionDuration(),
            stats.checkpointCount(),
            stats.restoreCount(),
            stats.lastCheckpointDuration(),
            stats.lastRestoreDuration()
        );
    }

    @Override
    public void close() {
        store.close();
    }

    private boolean matches(Txn txn) {
        if (txn.conditions().isEmpty()) {
            return true;
        }
        try (Snapshot view = store.snapshot()) {
            return txn.conditions().stream().allMatch(condition -> matches(view.get(condition.key()), condition));
        }
    }

    private static boolean matches(Optional<io.partdb.storage.KeyValue> current, Condition condition) {
        return switch (condition) {
            case Condition.Exists _ -> current.isPresent();
            case Condition.Missing _ -> current.isEmpty();
            case Condition.ValueEquals(_, var expected) -> current.map(io.partdb.storage.KeyValue::value).filter(expected::equals).isPresent();
            case Condition.RevisionEquals(_, long expected) -> current.filter(entry -> entry.revision() == expected).isPresent();
        };
    }

    private static io.partdb.storage.Mutation storageMutation(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Put(var key, var value) -> io.partdb.storage.Mutation.put(key, value);
            case Mutation.Delete(var key) -> io.partdb.storage.Mutation.delete(key);
        };
    }

    private static io.partdb.storage.KeyRange storageRange(KeyRange range) {
        return new io.partdb.storage.KeyRange(range.start(), range.end());
    }

    private static final class StorageScan implements Scan {
        private final Cursor entries;
        private final long revision;
        private int remaining;
        private boolean closed;

        private StorageScan(Cursor entries, int limit, long revision) {
            this.entries = entries;
            this.revision = revision;
            this.remaining = limit == 0 ? Integer.MAX_VALUE : limit;
        }

        @Override
        public long readRevision() {
            return revision;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (remaining > 0 && entries.hasNext()) {
                return true;
            }
            close();
            return false;
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            remaining--;
            var entry = entries.next();
            return new KeyValue(entry.key(), entry.value(), entry.revision());
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                entries.close();
            }
        }
    }
}
