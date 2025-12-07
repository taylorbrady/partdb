package io.partdb.server;

import com.google.protobuf.InvalidProtocolBufferException;
import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.KeyValue;
import io.partdb.common.Leases;
import io.partdb.common.CloseableIterator;
import io.partdb.raft.StateMachine;
import io.partdb.server.command.proto.CommandProto.Command;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

public final class KvStore implements StateMachine, AutoCloseable {

    private final LSMTree store;
    private final Leases leases;
    private PendingRequests pending;
    private volatile long lastApplied;

    private KvStore(LSMTree store, Leases leases) {
        this.store = store;
        this.leases = leases;
        this.lastApplied = 0;
    }

    public static KvStore open(Path dataDirectory, LSMConfig config) {
        LSMTree store = LSMTree.open(dataDirectory, config);
        Leases leases = new Leases();
        return new KvStore(store, leases);
    }

    public void setPendingRequests(PendingRequests pending) {
        this.pending = pending;
    }

    @Override
    public void apply(long index, byte[] data) {
        lastApplied = index;

        Command command;
        try {
            command = Command.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse command", e);
        }

        switch (command.getOpCase()) {
            case PUT -> {
                var put = command.getPut();
                ByteArray key = ByteArray.wrap(put.getKey().toByteArray());
                ByteArray value = ByteArray.wrap(put.getValue().toByteArray());
                long leaseId = put.getLeaseId();
                StoredValue stored = new StoredValue(value, index, leaseId);
                store.put(key, stored.encode());
                if (leaseId != 0) {
                    leases.attachKey(leaseId, key);
                }
            }
            case DELETE -> {
                var delete = command.getDelete();
                ByteArray key = ByteArray.wrap(delete.getKey().toByteArray());
                detachKeyFromLease(key);
                store.delete(key);
            }
            case GRANT_LEASE -> {
                var grant = command.getGrantLease();
                leases.grant(grant.getLeaseId(), grant.getTtlNanos());
            }
            case REVOKE_LEASE -> {
                var revoke = command.getRevokeLease();
                Set<ByteArray> keys = leases.getKeys(revoke.getLeaseId());
                for (ByteArray key : keys) {
                    store.delete(key);
                }
                leases.revoke(revoke.getLeaseId());
            }
            case KEEP_ALIVE_LEASE -> {
                var keepAlive = command.getKeepAliveLease();
                leases.keepAlive(keepAlive.getLeaseId());
            }
            case OP_NOT_SET -> throw new IllegalArgumentException("Command operation not set");
        }

        if (pending != null && command.getRequestId() != 0) {
            pending.complete(command.getRequestId());
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

    public CloseableIterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        CloseableIterator<KeyValue> raw = store.scan(startKey, endKey);
        return new DecodingIterator(raw, leases);
    }

    @Override
    public byte[] snapshot() {
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

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restore(long index, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.getLong();
        int storageLen = buffer.getInt();
        int leaseLen = buffer.getInt();

        byte[] storageData = new byte[storageLen];
        buffer.get(storageData);

        byte[] leaseData = new byte[leaseLen];
        buffer.get(leaseData);

        store.restore(storageData);
        leases.restoreSnapshot(leaseData);

        lastApplied = index;
    }

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
