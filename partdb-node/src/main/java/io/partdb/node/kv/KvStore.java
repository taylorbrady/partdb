package io.partdb.node.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.partdb.raft.StateMachine;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.lease.LeaseRegistry;
import io.partdb.storage.Entry;
import io.partdb.storage.LSMConfig;
import io.partdb.storage.LSMTree;
import io.partdb.storage.Slice;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public final class KvStore implements StateMachine, AutoCloseable {

    private final LSMTree store;
    private final LeaseRegistry leaseRegistry;
    private volatile long lastApplied;

    private KvStore(LSMTree store, LeaseRegistry leaseRegistry) {
        this.store = store;
        this.leaseRegistry = leaseRegistry;
        this.lastApplied = 0;
    }

    public static KvStore open(Path dataDirectory, LSMConfig config) {
        LSMTree store = LSMTree.open(dataDirectory, config);
        LeaseRegistry leaseRegistry = new LeaseRegistry();
        return new KvStore(store, leaseRegistry);
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
                Slice key = Slice.of(put.getKey().toByteArray());
                byte[] value = put.getValue().toByteArray();
                long leaseId = put.getLeaseId();
                StoredValue stored = new StoredValue(value, index, leaseId);
                store.put(key, Slice.of(stored.encode()), index);
                if (leaseId != 0) {
                    leaseRegistry.attachKey(leaseId, key);
                }
            }
            case DELETE -> {
                var delete = command.getDelete();
                Slice key = Slice.of(delete.getKey().toByteArray());
                detachKeyFromLease(key);
                store.delete(key, index);
            }
            case GRANT_LEASE -> {
                var grant = command.getGrantLease();
                leaseRegistry.grant(grant.getLeaseId(), grant.getTtlNanos());
            }
            case REVOKE_LEASE -> {
                var revoke = command.getRevokeLease();
                Set<Slice> keys = leaseRegistry.getKeys(revoke.getLeaseId());
                for (Slice key : keys) {
                    store.delete(key, index);
                }
                leaseRegistry.revoke(revoke.getLeaseId());
            }
            case KEEP_ALIVE_LEASE -> {
                var keepAlive = command.getKeepAliveLease();
                leaseRegistry.keepAlive(keepAlive.getLeaseId());
            }
            case OP_NOT_SET -> throw new IllegalArgumentException("Command operation not set");
        }
    }

    private void detachKeyFromLease(Slice key) {
        Optional<Entry> existing = store.get(key);
        if (existing.isPresent()) {
            StoredValue stored = StoredValue.decode(existing.get().value().toByteArray());
            if (stored.leaseId() != 0) {
                leaseRegistry.detachKey(stored.leaseId(), key);
            }
        }
    }

    public Optional<byte[]> get(byte[] keyBytes) {
        Optional<Entry> raw = store.get(Slice.of(keyBytes));
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        StoredValue stored = StoredValue.decode(raw.get().value().toByteArray());
        if (stored.leaseId() != 0 && !leaseRegistry.isLeaseActive(stored.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(stored.value());
    }

    public Stream<KvEntry> scan(byte[] startKeyBytes, byte[] endKeyBytes) {
        Slice startKey = startKeyBytes != null ? Slice.of(startKeyBytes) : null;
        Slice endKey = endKeyBytes != null ? Slice.of(endKeyBytes) : null;
        Stream<Entry> raw = store.scan(startKey, endKey);

        return raw
            .<KvEntry>mapMulti((entry, consumer) -> {
                StoredValue stored = StoredValue.decode(entry.value().toByteArray());
                if (stored.leaseId() == 0 || leaseRegistry.isLeaseActive(stored.leaseId())) {
                    consumer.accept(new KvEntry(
                        entry.key().toByteArray(),
                        stored.value(),
                        stored.version(),
                        stored.leaseId()
                    ));
                }
            });
    }

    public record KvEntry(byte[] key, byte[] value, long version, long leaseId) {}

    @Override
    public byte[] snapshot() {
        try {
            byte[] storageData = store.checkpoint();
            byte[] leaseData = leaseRegistry.toSnapshot();

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
        leaseRegistry.restoreSnapshot(leaseData);

        lastApplied = index;
    }

    public long lastAppliedIndex() {
        return lastApplied;
    }

    public LeaseRegistry leaseRegistry() {
        return leaseRegistry;
    }

    @Override
    public void close() {
        store.close();
    }
}
