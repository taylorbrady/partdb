package io.partdb.node.state;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.ApplyResult;
import io.partdb.consensus.ReplicatedStateMachine;
import io.partdb.node.internal.command.PartDbCommand;
import io.partdb.node.internal.command.PartDbCommandCodec;
import io.partdb.node.internal.command.PartDbCommandResult;
import io.partdb.node.internal.command.PartDbCommandResultCodec;
import io.partdb.node.kv.WriteBatchOperation;
import io.partdb.node.lease.LeaseId;
import io.partdb.node.lease.LeaseRegistry;
import io.partdb.node.recovery.RecoveryResult;
import io.partdb.storage.EntryRecord;
import io.partdb.storage.KeyRange;
import io.partdb.storage.Mutation;
import io.partdb.storage.Revision;
import io.partdb.storage.Scan;
import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageCheckpoint;
import io.partdb.storage.StorageEngine;
import io.partdb.storage.StorageStats;
import io.partdb.storage.ValueRecord;
import io.partdb.storage.WriteBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class PartDbStateMachine implements ReplicatedStateMachine, AutoCloseable {

    private final StorageEngine store;
    private final LeaseRegistry leaseRegistry;
    private volatile long lastApplied;

    private PartDbStateMachine(StorageEngine store, LeaseRegistry leaseRegistry) {
        this.store = store;
        this.leaseRegistry = leaseRegistry;
        this.lastApplied = 0;
    }

    public static PartDbStateMachine open(Path dataDirectory, StorageOptions options) {
        StorageEngine store = StorageEngine.open(dataDirectory, options);
        LeaseRegistry leaseRegistry = new LeaseRegistry();
        return new PartDbStateMachine(store, leaseRegistry);
    }

    @Override
    public ApplyResult apply(long index, Bytes data) {
        PartDbCommand command = PartDbCommandCodec.decode(data);

        ApplyResult result = switch (command) {
            case PartDbCommand.Put(var key, var value, long leaseId) -> {
                if (leaseId != 0 && !leaseRegistry.isLeaseActive(leaseId)) {
                    yield rejected(new PartDbCommandResult.LeaseNotFound(leaseId));
                }
                detachKeyFromLease(key);
                StoredValue stored = new StoredValue(value.toByteArray(), leaseId);
                store.apply(new Revision(index), Mutation.put(key, Bytes.copyOf(stored.encode())));
                if (leaseId != 0) {
                    leaseRegistry.attachKey(leaseId, key);
                }
                yield applied(new PartDbCommandResult.PutApplied(index));
            }
            case PartDbCommand.Delete(var key) -> {
                detachKeyFromLease(key);
                store.apply(new Revision(index), Mutation.delete(key));
                yield applied(new PartDbCommandResult.DeleteApplied(index));
            }
            case PartDbCommand.BatchWrite(var batch) -> {
                for (WriteBatchOperation operation : batch.operations()) {
                    if (operation instanceof WriteBatchOperation.Put put
                        && put.leaseId().isPresent()
                        && !leaseRegistry.isLeaseActive(put.leaseId().orElseThrow().value())) {
                        yield rejected(new PartDbCommandResult.LeaseNotFound(put.leaseId().orElseThrow().value()));
                    }
                }

                WriteBatch.Builder storageBatch = WriteBatch.builder();
                var attachments = new ArrayList<LeaseAttachment>(batch.operations().size());

                for (WriteBatchOperation operation : batch.operations()) {
                    detachKeyFromLease(operation.key());
                    switch (operation) {
                        case WriteBatchOperation.Put(var key, var value, var leaseId) -> {
                            long storedLeaseId = leaseId.map(LeaseId::value).orElse(0L);
                            StoredValue stored = new StoredValue(value.toByteArray(), storedLeaseId);
                            storageBatch.put(key, Bytes.copyOf(stored.encode()));
                            if (storedLeaseId != 0) {
                                attachments.add(new LeaseAttachment(storedLeaseId, key));
                            }
                        }
                        case WriteBatchOperation.Delete(var key) -> storageBatch.delete(key);
                    }
                }

                store.apply(new Revision(index), storageBatch.build());
                for (LeaseAttachment attachment : attachments) {
                    leaseRegistry.attachKey(attachment.leaseId(), attachment.key());
                }

                yield applied(new PartDbCommandResult.BatchWriteApplied(index));
            }
            case PartDbCommand.GrantLease(long ttlNanos) -> {
                leaseRegistry.grant(index, ttlNanos);
                yield applied(new PartDbCommandResult.LeaseGranted(index, index, ttlNanos));
            }
            case PartDbCommand.RevokeLease(long leaseId) -> {
                if (!leaseRegistry.isLeaseActive(leaseId)) {
                    yield rejected(new PartDbCommandResult.LeaseNotFound(leaseId));
                }
                yield applied(revokeLease(index, leaseId));
            }
            case PartDbCommand.ExpireLease(long leaseId) -> applied(
                leaseRegistry.isLeaseActive(leaseId)
                    ? revokeLease(index, leaseId)
                    : new PartDbCommandResult.LeaseRevoked(index, leaseId, 0)
            );
            case PartDbCommand.KeepAliveLease(long leaseId) -> {
                var ttlNanos = leaseRegistry.keepAlive(leaseId);
                if (ttlNanos.isEmpty()) {
                    yield rejected(new PartDbCommandResult.LeaseNotFound(leaseId));
                }
                yield applied(new PartDbCommandResult.LeaseKeptAlive(index, leaseId, ttlNanos.getAsLong()));
            }
        };

        lastApplied = index;
        return result;
    }

    private void detachKeyFromLease(Bytes key) {
        Optional<ValueRecord> existing = store.get(key);
        if (existing.isPresent()) {
            StoredValue stored = StoredValue.decode(existing.get().value().toByteArray());
            if (stored.leaseId() != 0) {
                leaseRegistry.detachKey(stored.leaseId(), key);
            }
        }
    }

    private PartDbCommandResult.LeaseRevoked revokeLease(long index, long leaseId) {
        WriteBatch.Builder batch = WriteBatch.builder();
        var attachedKeys = leaseRegistry.attachedKeys(leaseId);
        for (Bytes key : attachedKeys) {
            batch.delete(key);
        }
        store.apply(new Revision(index), batch.build());
        leaseRegistry.revoke(leaseId);
        return new PartDbCommandResult.LeaseRevoked(index, leaseId, attachedKeys.size());
    }

    public Optional<Bytes> getLocal(Bytes key) {
        return getLocalValue(key).map(LocalValue::value);
    }

    public Optional<LocalValue> getLocalValue(Bytes key) {
        Optional<ValueRecord> raw = store.get(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        StoredValue stored = StoredValue.decode(raw.get().value().toByteArray());
        if (stored.leaseId() != 0 && !leaseRegistry.isLeaseActive(stored.leaseId())) {
            return Optional.empty();
        }
        return Optional.of(new LocalValue(
            Bytes.copyOf(stored.value()),
            raw.get().modRevision().value(),
            stored.leaseId()
        ));
    }

    public Stream<KvEntry> scanLocal(KeyRange range) {
        Scan cursor = store.scan(range);
        Iterator<EntryRecord> entries = cursor.iterator();

        Iterator<KvEntry> iterator = new Iterator<>() {
            private KvEntry next = advance();

            private KvEntry advance() {
                while (entries.hasNext()) {
                    EntryRecord entry = entries.next();
                    StoredValue stored = StoredValue.decode(entry.value().toByteArray());
                    if (stored.leaseId() == 0 || leaseRegistry.isLeaseActive(stored.leaseId())) {
                        return new KvEntry(
                            entry.key(),
                            Bytes.copyOf(stored.value()),
                            entry.modRevision().value(),
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

    public record LocalValue(Bytes value, long modRevision, long leaseId) {}

    private record LeaseAttachment(long leaseId, Bytes key) {}

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

        store.restore(new StorageCheckpoint(Bytes.copyOf(storageData)));
        leaseRegistry.restoreSnapshot(leaseData);
        rebuildLeaseAttachments();

        lastApplied = index;
    }

    public long lastAppliedIndex() {
        return lastApplied;
    }

    public RecoveryResult invalidateLeasesForDisasterRecovery() {
        long invalidatedLeaseCount = leaseRegistry.leaseCount();
        WriteBatch.Builder batch = WriteBatch.builder();
        long deletedLeaseAttachedKeys = 0;

        try (Scan cursor = store.scan(KeyRange.all())) {
            for (EntryRecord entry : cursor) {
                StoredValue stored = StoredValue.decode(entry.value().toByteArray());
                if (stored.leaseId() != 0) {
                    batch.delete(entry.key());
                    deletedLeaseAttachedKeys++;
                }
            }
        }

        if (deletedLeaseAttachedKeys > 0) {
            long recoveryRevision = lastApplied + 1;
            store.apply(new Revision(recoveryRevision), batch.build());
            lastApplied = recoveryRevision;
        }

        leaseRegistry.clear();
        return new RecoveryResult(lastApplied, invalidatedLeaseCount, deletedLeaseAttachedKeys);
    }

    public LeaseRegistry leaseRegistry() {
        return leaseRegistry;
    }

    public StorageStats storageStats() {
        return store.stats();
    }

    @Override
    public void close() {
        store.close();
    }

    private void rebuildLeaseAttachments() {
        try (Scan cursor = store.scan(KeyRange.all())) {
            for (EntryRecord entry : cursor) {
                StoredValue stored = StoredValue.decode(entry.value().toByteArray());
                if (stored.leaseId() != 0 && leaseRegistry.isLeaseActive(stored.leaseId())) {
                    leaseRegistry.attachKey(stored.leaseId(), entry.key());
                }
            }
        }
    }

    private static ApplyResult applied(PartDbCommandResult.AppliedCommandResult result) {
        return new ApplyResult.Applied(PartDbCommandResultCodec.encode(result));
    }

    private static ApplyResult rejected(PartDbCommandResult.RejectedCommandResult result) {
        return new ApplyResult.Rejected(PartDbCommandResultCodec.encode(result));
    }
}
