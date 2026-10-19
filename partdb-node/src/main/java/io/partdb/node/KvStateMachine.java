package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.raft.runtime.StateMachine;
import io.partdb.storage.KeyRange;
import io.partdb.storage.KeyValue;
import io.partdb.storage.Snapshot;
import io.partdb.storage.Store;
import io.partdb.storage.StoreOptions;
import io.partdb.storage.StoreStats;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

final class KvStateMachine implements StateMachine<TxnResult>, AutoCloseable {

    private final Store store;

    private KvStateMachine(Store store) {
        this.store = store;
    }

    static KvStateMachine open(Path directory) {
        return new KvStateMachine(Store.open(directory, StoreOptions.defaults()));
    }

    @Override
    public long appliedIndex() {
        return store.appliedRevision();
    }

    @Override
    public long durableIndex() {
        return store.durableRevision();
    }

    @Override
    public TxnResult apply(long index, Bytes command) {
        Txn txn = TxnCodec.decode(command);
        if (!matches(txn)) {
            store.apply(index, List.of());
            return new TxnResult.ConditionFailed();
        }
        store.apply(index, txn.mutations());
        return new TxnResult.Committed(index);
    }

    @Override
    public void skip(long index) {
        store.apply(index, List.of());
    }

    @Override
    public CompletionStage<Long> flush() {
        return store.flush();
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

    Read<Optional<KeyValue>> get(Bytes key) {
        try (Snapshot snapshot = store.snapshot()) {
            return new Read<>(snapshot.get(key), snapshot.revision());
        }
    }

    Read<Stream<KeyValue>> scan(KeyRange range) {
        try (Snapshot snapshot = store.snapshot()) {
            return new Read<>(snapshot.scan(range), snapshot.revision());
        }
    }

    StoreStats stats() {
        return store.stats();
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

    private static boolean matches(Optional<KeyValue> current, Condition condition) {
        return switch (condition) {
            case Condition.Exists _ -> current.isPresent();
            case Condition.Missing _ -> current.isEmpty();
            case Condition.ValueEquals(_, var expected) -> current.map(KeyValue::value).filter(expected::equals).isPresent();
            case Condition.RevisionEquals(_, long expected) -> current.filter(entry -> entry.revision() == expected).isPresent();
        };
    }
}
