package io.partdb.node.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.lease.LeaseRegistry;
import io.partdb.node.raft.StateMachine;
import io.partdb.storage.StateStore;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.StorageCursor;
import io.partdb.storage.StorageEngineStats;
import io.partdb.storage.StorageSnapshot;
import io.partdb.storage.VersionedEntry;

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

public final class KvStore implements StateMachine, AutoCloseable {

    private final StateStore store;
    private final LeaseRegistry leaseRegistry;
    private volatile long lastApplied;

    private KvStore(StateStore store, LeaseRegistry leaseRegistry) {
        this.store = store;
        this.leaseRegistry = leaseRegistry;
        this.lastApplied = 0;
    }

    public static KvStore open(Path dataDirectory, StorageConfig config) {
        StateStore store = StateStore.open(dataDirectory, config);
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
                byte[] key = put.getKey().toByteArray();
                byte[] value = put.getValue().toByteArray();
                long leaseId = put.getLeaseId();
                StoredValue stored = new StoredValue(value, index, leaseId);
                store.put(key, stored.encode(), index);
                if (leaseId != 0) {
                    leaseRegistry.attachKey(leaseId, key);
                }
            }
            case DELETE -> {
                var delete = command.getDelete();
                byte[] key = delete.getKey().toByteArray();
                detachKeyFromLease(key);
                store.delete(key, index);
            }
            case GRANT_LEASE -> {
                var grant = command.getGrantLease();
                leaseRegistry.grant(grant.getLeaseId(), grant.getTtlNanos());
            }
            case REVOKE_LEASE -> {
                var revoke = command.getRevokeLease();
                for (byte[] key : leaseRegistry.attachedKeys(revoke.getLeaseId())) {
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

    private void detachKeyFromLease(byte[] key) {
        Optional<VersionedEntry> existing = store.get(key);
        if (existing.isPresent()) {
            StoredValue stored = StoredValue.decode(existing.get().value());
            if (stored.leaseId() != 0) {
                leaseRegistry.detachKey(stored.leaseId(), key);
            }
        }
    }

    public Optional<byte[]> get(byte[] keyBytes) {
        Optional<VersionedEntry> raw = store.get(keyBytes);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        StoredValue stored = StoredValue.decode(raw.get().value());
        if (stored.leaseId() != 0 && !leaseRegistry.isLeaseActive(stored.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(stored.value());
    }

    public Stream<KvEntry> scan(byte[] startKeyBytes, byte[] endKeyBytes) {
        StorageCursor cursor = store.scan(startKeyBytes, endKeyBytes);

        Iterator<KvEntry> iterator = new Iterator<>() {
            private KvEntry next = advance();

            private KvEntry advance() {
                while (cursor.hasNext()) {
                    VersionedEntry entry = cursor.next();
                    StoredValue stored = StoredValue.decode(entry.value());
                    if (stored.leaseId() == 0 || leaseRegistry.isLeaseActive(stored.leaseId())) {
                        return new KvEntry(entry.key(), stored.value(), stored.version(), stored.leaseId());
                    }
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

    public record KvEntry(byte[] key, byte[] value, long version, long leaseId) {}

    @Override
    public byte[] snapshot() {
        try {
            byte[] storageData = store.snapshot().bytes();
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

        store.restore(new StorageSnapshot(storageData));
        leaseRegistry.restoreSnapshot(leaseData);
        rebuildLeaseAttachments();

        lastApplied = index;
    }

    public long lastAppliedIndex() {
        return lastApplied;
    }

    public LeaseRegistry leaseRegistry() {
        return leaseRegistry;
    }

    public StorageEngineStats storageStats() {
        return store.stats();
    }

    @Override
    public void close() {
        store.close();
    }

    private void rebuildLeaseAttachments() {
        try (StorageCursor cursor = store.scan(null, null)) {
            while (cursor.hasNext()) {
                VersionedEntry entry = cursor.next();
                StoredValue stored = StoredValue.decode(entry.value());
                if (stored.leaseId() != 0 && leaseRegistry.isLeaseActive(stored.leaseId())) {
                    leaseRegistry.attachKey(stored.leaseId(), entry.key());
                }
            }
        }
    }
}
