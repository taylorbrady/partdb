package io.partdb.server;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.KeyValue;
import io.partdb.common.Leases;
import io.partdb.common.statemachine.*;
import io.partdb.common.CloseableIterator;
import io.partdb.storage.Store;
import io.partdb.storage.StoreConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

public final class Database implements StateMachine, AutoCloseable {

    private final Store store;
    private final Leases leases;
    private volatile long lastApplied;

    private Database(Store store, Leases leases) {
        this.store = store;
        this.leases = leases;
        this.lastApplied = 0;
    }

    public static Database open(Path dataDirectory, StoreConfig config) {
        Store store = Store.open(dataDirectory, config);
        Leases leases = new Leases();
        return new Database(store, leases);
    }

    @Override
    public void apply(long index, Operation operation) {
        lastApplied = index;

        switch (operation) {
            case Put put -> {
                StoredValue stored = new StoredValue(put.value(), index, put.leaseId());
                store.put(put.key(), stored.encode());
                if (put.leaseId() != 0) {
                    leases.attachKey(put.leaseId(), put.key());
                }
            }
            case Delete delete -> {
                detachKeyFromLease(delete.key());
                store.delete(delete.key());
            }
            case GrantLease grant -> {
                leases.grant(grant.leaseId(), grant.ttlMillis(), grant.grantedAtMillis());
            }
            case RevokeLease revoke -> {
                Set<ByteArray> keys = leases.getKeys(revoke.leaseId());
                for (ByteArray key : keys) {
                    store.delete(key);
                }
                leases.revoke(revoke.leaseId());
            }
            case KeepAliveLease keepAlive -> {
                leases.keepAlive(keepAlive.leaseId(), System.currentTimeMillis());
            }
        }
    }

    private void detachKeyFromLease(ByteArray key) {
        Optional<ByteArray> existing = store.get(key);
        if (existing.isPresent()) {
            StoredValue stored = StoredValue.decode(existing.get());
            if (stored.leaseId() != 0) {
                leases.detachKey(stored.leaseId(), key);
            }
        }
    }

    @Override
    public Optional<ByteArray> get(ByteArray key) {
        Optional<ByteArray> raw = store.get(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        StoredValue stored = StoredValue.decode(raw.get());
        if (stored.leaseId() != 0 && !leases.isLeaseActive(stored.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(stored.value());
    }

    @Override
    public CloseableIterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        CloseableIterator<KeyValue> raw = store.scan(startKey, endKey);
        return new DecodingIterator(raw, leases);
    }

    @Override
    public StateSnapshot snapshot() {
        store.flush();

        try {
            byte[] storageData = store.snapshot();
            byte[] leaseData = leases.toSnapshot();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(16);
            header.putLong(lastApplied);
            header.putInt(storageData.length);
            header.putInt(leaseData.length);
            baos.write(header.array());
            baos.write(storageData);
            baos.write(leaseData);

            return StateSnapshot.create(lastApplied, baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restore(StateSnapshot snapshot) {
        ByteBuffer buffer = ByteBuffer.wrap(snapshot.data());
        long snapshotIndex = buffer.getLong();
        int storageLen = buffer.getInt();
        int leaseLen = buffer.getInt();

        byte[] storageData = new byte[storageLen];
        buffer.get(storageData);

        byte[] leaseData = new byte[leaseLen];
        buffer.get(leaseData);

        store.restore(storageData);
        leases.restoreSnapshot(leaseData);

        rebuildLeaseKeyIndex();

        lastApplied = snapshotIndex;
    }

    private void rebuildLeaseKeyIndex() {
        try (CloseableIterator<KeyValue> iter = store.scan(null, null)) {
            while (iter.hasNext()) {
                KeyValue kv = iter.next();
                StoredValue stored = StoredValue.decode(kv.value());
                if (stored.leaseId() != 0 && leases.isLeaseActive(stored.leaseId())) {
                    leases.attachKey(stored.leaseId(), kv.key());
                }
            }
        }
    }

    @Override
    public long lastAppliedIndex() {
        return lastApplied;
    }

    public Leases leases() {
        return leases;
    }

    @Override
    public void close() {
        store.close();
    }

    private static final class DecodingIterator implements CloseableIterator<Entry> {

        private final CloseableIterator<KeyValue> delegate;
        private final Leases leases;
        private Entry next;

        DecodingIterator(CloseableIterator<KeyValue> delegate, Leases leases) {
            this.delegate = delegate;
            this.leases = leases;
            advance();
        }

        private void advance() {
            while (delegate.hasNext()) {
                KeyValue kv = delegate.next();
                StoredValue stored = StoredValue.decode(kv.value());
                if (stored.leaseId() != 0 && !leases.isLeaseActive(stored.leaseId())) {
                    continue;
                }
                next = Entry.putWithLease(kv.key(), stored.value(), stored.version(), stored.leaseId());
                return;
            }
            next = null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            Entry result = next;
            advance();
            return result;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
