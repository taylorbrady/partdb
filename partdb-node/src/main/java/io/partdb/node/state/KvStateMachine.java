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
import io.partdb.storage.EntryRecord;
import io.partdb.storage.MutationBatch;
import io.partdb.storage.Revision;
import io.partdb.storage.StorageEngine;
import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageReadView;
import io.partdb.storage.ValueRecord;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class KvStateMachine implements StateMachine<TxnResult>, AutoCloseable {

    private final StorageEngine store;

    private KvStateMachine(StorageEngine store) {
        this.store = store;
    }

    public static KvStateMachine open(Path directory) {
        return new KvStateMachine(StorageEngine.open(directory, StorageOptions.defaults()));
    }

    @Override
    public long appliedIndex() {
        return store.appliedThrough().value();
    }

    @Override
    public TxnResult apply(long index, Bytes command) {
        Txn txn = TxnCodec.decode(command);
        if (!matches(txn)) {
            return new TxnResult.ConditionFailed();
        }
        var batch = MutationBatch.builder();
        for (Mutation mutation : txn.mutations()) {
            switch (mutation) {
                case Mutation.Put(var key, var value) -> batch.put(key, value);
                case Mutation.Delete(var key) -> batch.delete(key);
            }
        }
        store.apply(new Revision(index), batch.build());
        return new TxnResult.Committed(index);
    }

    @Override
    public void skip(long index) {
    }

    @Override
    public long snapshot(OutputStream target) {
        return store.checkpoint(target).value();
    }

    @Override
    public void restore(long index, InputStream snapshot) {
        store.restore(snapshot);
        long restored = store.appliedThrough().value();
        if (restored != index) {
            throw new IllegalStateException("snapshot " + index + " restored storage at " + restored);
        }
    }

    public Read<Optional<KeyValue>> get(Bytes key) {
        long revision = appliedIndex();
        return new Read<>(store.get(key).map(record -> new KeyValue(key, record.value(), record.modRevision().value())), revision);
    }

    public Scan scan(KeyRange range, int limit) {
        long revision = appliedIndex();
        return new StorageScan(store.scan(storageRange(range)), limit, revision);
    }

    public StorageMetrics metrics() {
        var stats = store.stats();
        return new StorageMetrics(
            stats.activeMemtableBytes(),
            stats.immutableMemtableCount(),
            stats.sstableCount(),
            stats.totalSstableBytes(),
            stats.activeCompactions(),
            stats.completedCompactions(),
            stats.failedCompactions(),
            Duration.ofMillis(stats.lastCompactionDurationMillis()),
            stats.checkpointCount(),
            stats.restoreCount(),
            Duration.ofMillis(stats.lastCheckpointDurationMillis()),
            Duration.ofMillis(stats.lastRestoreDurationMillis())
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
        try (StorageReadView view = store.readView()) {
            return txn.conditions().stream().allMatch(condition -> matches(view.get(condition.key()), condition));
        }
    }

    private static boolean matches(Optional<ValueRecord> current, Condition condition) {
        return switch (condition) {
            case Condition.Exists _ -> current.isPresent();
            case Condition.Missing _ -> current.isEmpty();
            case Condition.ValueEquals(_, var expected) -> current.map(ValueRecord::value).filter(expected::equals).isPresent();
            case Condition.RevisionEquals(_, long expected) -> current.filter(value -> value.modRevision().value() == expected).isPresent();
        };
    }

    private static io.partdb.storage.KeyRange storageRange(KeyRange range) {
        boolean open = range.start().isEmpty();
        return switch (range.end().orElse(null)) {
            case null -> open ? io.partdb.storage.KeyRange.all() : io.partdb.storage.KeyRange.from(range.start());
            case Bytes end -> open ? io.partdb.storage.KeyRange.until(end) : io.partdb.storage.KeyRange.between(range.start(), end);
        };
    }

    private static final class StorageScan implements Scan {
        private final io.partdb.storage.Scan scan;
        private final Iterator<EntryRecord> entries;
        private final long revision;
        private int remaining;
        private boolean closed;

        private StorageScan(io.partdb.storage.Scan scan, int limit, long revision) {
            this.scan = scan;
            this.entries = scan.iterator();
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
            EntryRecord entry = entries.next();
            return new KeyValue(entry.key(), entry.value(), entry.modRevision().value());
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
