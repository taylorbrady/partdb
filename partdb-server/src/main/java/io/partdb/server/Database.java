package io.partdb.server;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.Leases;
import io.partdb.common.statemachine.*;
import io.partdb.storage.CompactionFilter;
import io.partdb.storage.Store;
import io.partdb.storage.StoreConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class Database implements StateMachine, AutoCloseable {

    private final Store store;
    private final Leases leases;
    private volatile long lastApplied;

    private Database(Store store, Leases leases, long lastApplied) {
        this.store = store;
        this.leases = leases;
        this.lastApplied = lastApplied;
    }

    public static Database open(Path dataDirectory, StoreConfig config) {
        Leases leases = new Leases();

        CompactionFilter filter = entry ->
            entry.leaseId() == 0 || leases.isLeaseActive(entry.leaseId());

        StoreConfig configWithFilter = new StoreConfig(
            config.memtableConfig(),
            config.sstableConfig(),
            filter
        );

        Store store = Store.open(dataDirectory, configWithFilter);
        return new Database(store, leases, store.lastAppliedIndex());
    }

    @Override
    public void apply(long index, Operation operation) {
        lastApplied = index;
        store.setLastAppliedIndex(index);

        switch (operation) {
            case Put put -> {
                Entry entry = Entry.putWithLease(put.key(), put.value(), index, put.leaseId());
                store.put(entry);
            }
            case Delete delete -> {
                Entry entry = Entry.delete(delete.key(), index);
                store.put(entry);
            }
            case GrantLease grant -> {
                leases.grant(grant.leaseId(), grant.ttlMillis(), grant.grantedAtMillis());
            }
            case RevokeLease revoke -> {
                leases.revoke(revoke.leaseId());
            }
            case KeepAliveLease keepAlive -> {
                leases.keepAlive(keepAlive.leaseId(), System.currentTimeMillis());
            }
        }
    }

    @Override
    public Optional<ByteArray> get(ByteArray key) {
        Optional<Entry> entry = store.get(key);
        if (entry.isEmpty()) {
            return Optional.empty();
        }
        Entry e = entry.get();
        if (e.tombstone()) {
            return Optional.empty();
        }
        if (e.leaseId() != 0 && !leases.isLeaseActive(e.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(e.value());
    }

    @Override
    public Iterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        Iterator<Entry> raw = store.scan(startKey, endKey);
        return new FilteringIterator(raw, leases);
    }

    @Override
    public StateSnapshot snapshot() {
        store.flush();

        try {
            byte[] storageData = store.toSnapshot(lastApplied);
            byte[] leaseData = leases.toSnapshot();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(8);
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
        int storageLen = buffer.getInt();
        int leaseLen = buffer.getInt();

        byte[] storageData = new byte[storageLen];
        buffer.get(storageData);

        byte[] leaseData = new byte[leaseLen];
        buffer.get(leaseData);

        long checkpoint = store.restoreSnapshot(storageData);
        leases.restoreSnapshot(leaseData);

        lastApplied = checkpoint;
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

    private static final class FilteringIterator implements Iterator<Entry> {

        private final Iterator<Entry> delegate;
        private final Leases leases;
        private Entry next;

        FilteringIterator(Iterator<Entry> delegate, Leases leases) {
            this.delegate = delegate;
            this.leases = leases;
            advance();
        }

        private void advance() {
            while (delegate.hasNext()) {
                Entry candidate = delegate.next();
                if (candidate.tombstone()) {
                    continue;
                }
                if (candidate.leaseId() != 0 && !leases.isLeaseActive(candidate.leaseId())) {
                    continue;
                }
                next = candidate;
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
    }
}
