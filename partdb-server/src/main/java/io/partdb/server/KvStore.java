package io.partdb.server;

import com.google.protobuf.InvalidProtocolBufferException;
import io.partdb.common.Entry;
import io.partdb.common.KeyBytes;
import io.partdb.common.Leases;
import io.partdb.common.Timestamp;
import io.partdb.raft.StateMachine;
import io.partdb.server.command.proto.CommandProto.Command;
import io.partdb.storage.KeyValue;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
                byte[] key = put.getKey().toByteArray();
                byte[] value = put.getValue().toByteArray();
                long leaseId = put.getLeaseId();
                StoredValue stored = new StoredValue(value, index, leaseId);
                store.put(key, stored.encode(), Timestamp.of(index, 0));
                if (leaseId != 0) {
                    leases.attachKey(leaseId, key);
                }
            }
            case DELETE -> {
                var delete = command.getDelete();
                byte[] key = delete.getKey().toByteArray();
                detachKeyFromLease(key);
                store.delete(key, Timestamp.of(index, 0));
            }
            case GRANT_LEASE -> {
                var grant = command.getGrantLease();
                leases.grant(grant.getLeaseId(), grant.getTtlNanos());
            }
            case REVOKE_LEASE -> {
                var revoke = command.getRevokeLease();
                Set<KeyBytes> keys = leases.getKeys(revoke.getLeaseId());
                for (KeyBytes key : keys) {
                    store.delete(key.data(), Timestamp.of(index, 0));
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

    private void detachKeyFromLease(byte[] key) {
        try (var snap = store.snapshot(Timestamp.MAX)) {
            Optional<KeyValue> existing = snap.get(key);
            if (existing.isPresent()) {
                StoredValue stored = StoredValue.decode(existing.get().value());
                if (stored.leaseId() != 0) {
                    leases.detachKey(stored.leaseId(), key);
                }
            }
        }
    }

    public Optional<byte[]> get(byte[] key) {
        try (var snap = store.snapshot(Timestamp.MAX)) {
            Optional<KeyValue> raw = snap.get(key);
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            StoredValue stored = StoredValue.decode(raw.get().value());
            if (stored.leaseId() != 0 && !leases.isLeaseActive(stored.leaseId())) {
                return Optional.empty();
            }
            return Optional.of(stored.value());
        }
    }

    public Stream<Entry> scan(byte[] startKey, byte[] endKey) {
        var snap = store.snapshot(Timestamp.MAX);
        Stream<KeyValue> raw = snap.scan(startKey, endKey);

        return raw
            .<Entry>mapMulti((kv, consumer) -> {
                StoredValue stored = StoredValue.decode(kv.value());
                if (stored.leaseId() == 0 || leases.isLeaseActive(stored.leaseId())) {
                    consumer.accept(Entry.putWithLease(kv.key(), stored.value(), stored.version(), stored.leaseId()));
                }
            })
            .onClose(snap::close);
    }

    @Override
    public byte[] snapshot() {
        store.flush();

        try {
            byte[] storageData = store.checkpoint();
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

        store.restoreFromCheckpoint(storageData);
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
}
