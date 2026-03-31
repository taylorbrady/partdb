package io.partdb.node.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.partdb.bytes.Bytes;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.lease.LeaseRegistry;
import io.partdb.node.raft.StateMachine;
import io.partdb.storage.EntryCursor;
import io.partdb.storage.KeyRange;
import io.partdb.storage.LsmStats;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.StorageCheckpoint;
import io.partdb.storage.VersionedKeyValueStore;
import io.partdb.storage.VersionedEntry;
import io.partdb.storage.VersionedValue;

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

    private final VersionedKeyValueStore store;
    private final LeaseRegistry leaseRegistry;
    private volatile long lastApplied;

    private KvStore(VersionedKeyValueStore store, LeaseRegistry leaseRegistry) {
        this.store = store;
        this.leaseRegistry = leaseRegistry;
        this.lastApplied = 0;
    }

    public static KvStore open(Path dataDirectory, StorageConfig config) {
        VersionedKeyValueStore store = VersionedKeyValueStore.open(dataDirectory, config);
        LeaseRegistry leaseRegistry = new LeaseRegistry();
        return new KvStore(store, leaseRegistry);
    }

    @Override
    public void apply(long index, Bytes data) {
        lastApplied = index;

        Command command;
        try {
            command = Command.parseFrom(data.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse command", e);
        }

        switch (command.getOpCase()) {
            case PUT -> {
                var put = command.getPut();
                Bytes key = Bytes.copyOf(put.getKey().toByteArray());
                Bytes value = Bytes.copyOf(put.getValue().toByteArray());
                long leaseId = put.getLeaseId();
                StoredValue stored = new StoredValue(value.toByteArray(), index, leaseId);
                store.put(key, Bytes.copyOf(stored.encode()), index);
                if (leaseId != 0) {
                    leaseRegistry.attachKey(leaseId, key);
                }
            }
            case DELETE -> {
                var delete = command.getDelete();
                Bytes key = Bytes.copyOf(delete.getKey().toByteArray());
                detachKeyFromLease(key);
                store.delete(key, index);
            }
            case GRANT_LEASE -> {
                var grant = command.getGrantLease();
                leaseRegistry.grant(grant.getLeaseId(), grant.getTtlNanos());
            }
            case REVOKE_LEASE -> {
                var revoke = command.getRevokeLease();
                for (Bytes key : leaseRegistry.attachedKeys(revoke.getLeaseId())) {
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

    private void detachKeyFromLease(Bytes key) {
        Optional<VersionedValue> existing = store.get(key);
        if (existing.isPresent()) {
            StoredValue stored = StoredValue.decode(existing.get().value().toByteArray());
            if (stored.leaseId() != 0) {
                leaseRegistry.detachKey(stored.leaseId(), key);
            }
        }
    }

    public Optional<Bytes> get(Bytes key) {
        Optional<VersionedValue> raw = store.get(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        StoredValue stored = StoredValue.decode(raw.get().value().toByteArray());
        if (stored.leaseId() != 0 && !leaseRegistry.isLeaseActive(stored.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(Bytes.copyOf(stored.value()));
    }

    public Stream<KvEntry> scan(KeyRange range) {
        EntryCursor cursor = store.scan(range);

        Iterator<KvEntry> iterator = new Iterator<>() {
            private KvEntry next = advance();

            private KvEntry advance() {
                while (cursor.hasNext()) {
                    VersionedEntry entry = cursor.next();
                    StoredValue stored = StoredValue.decode(entry.value().toByteArray());
                    if (stored.leaseId() == 0 || leaseRegistry.isLeaseActive(stored.leaseId())) {
                        return new KvEntry(
                            entry.key(),
                            Bytes.copyOf(stored.value()),
                            stored.version(),
                            stored.leaseId()
                        );
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

    public record KvEntry(Bytes key, Bytes value, long version, long leaseId) {}

    @Override
    public Bytes snapshot() {
        try {
            byte[] storageData = store.checkpoint().bytes().toByteArray();
            byte[] leaseData = leaseRegistry.toSnapshot();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(16);
            header.putLong(lastApplied);
            header.putInt(storageData.length);
            header.putInt(leaseData.length);
            baos.write(header.array());
            baos.write(storageData);
            baos.write(leaseData);

            return Bytes.copyOf(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot", e);
        }
    }

    @Override
    public void restore(long index, Bytes data) {
        ByteBuffer buffer = ByteBuffer.wrap(data.toByteArray());
        buffer.getLong();
        int storageLen = buffer.getInt();
        int leaseLen = buffer.getInt();

        byte[] storageData = new byte[storageLen];
        buffer.get(storageData);

        byte[] leaseData = new byte[leaseLen];
        buffer.get(leaseData);

        store.replaceWith(new StorageCheckpoint(Bytes.copyOf(storageData)));
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

    public LsmStats storageStats() {
        return store.stats();
    }

    @Override
    public void close() {
        store.close();
    }

    private void rebuildLeaseAttachments() {
        try (EntryCursor cursor = store.scan(KeyRange.all())) {
            while (cursor.hasNext()) {
                VersionedEntry entry = cursor.next();
                StoredValue stored = StoredValue.decode(entry.value().toByteArray());
                if (stored.leaseId() != 0 && leaseRegistry.isLeaseActive(stored.leaseId())) {
                    leaseRegistry.attachKey(stored.leaseId(), entry.key());
                }
            }
        }
    }
}
