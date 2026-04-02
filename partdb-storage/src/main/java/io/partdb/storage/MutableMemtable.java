package io.partdb.storage;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

final class MutableMemtable implements Memtable {

    private static final int ENTRY_OVERHEAD = 100;

    private final ConcurrentSkipListMap<InternalKey, InternalEntry> entries;
    private final AtomicLong sizeInBytes;
    private final AtomicLong maxRevision;
    private final ReentrantLock lifecycleLock;

    private volatile ImmutableMemtable immutableView;

    MutableMemtable() {
        this.entries = new ConcurrentSkipListMap<>();
        this.sizeInBytes = new AtomicLong(0);
        this.maxRevision = new AtomicLong(0);
        this.lifecycleLock = new ReentrantLock();
        this.immutableView = null;
    }

    WriteResult put(InternalEntry entry) {
        lifecycleLock.lock();
        try {
            if (immutableView != null) {
                return WriteResult.FROZEN;
            }

            final long[] delta = {0};
            final WriteResult[] result = new WriteResult[1];

            entries.compute(entry.key(), (_, existing) -> {
                if (existing == null) {
                    delta[0] = estimatedHeapBytes(entry);
                    result[0] = WriteResult.APPLIED;
                    return entry;
                }

                if (existing.equals(entry)) {
                    result[0] = WriteResult.DUPLICATE;
                    return existing;
                }

                throw invalidRevision(entry, existing);
            });

            if (delta[0] != 0) {
                sizeInBytes.addAndGet(delta[0]);
                maxRevision.accumulateAndGet(entry.revision(), Math::max);
            }
            return result[0];
        } finally {
            lifecycleLock.unlock();
        }
    }

    WriteResult put(StoredEntry entry) {
        return put(InternalEntry.from(entry));
    }

    ImmutableMemtable freeze() {
        lifecycleLock.lock();
        try {
            if (immutableView == null) {
                immutableView = new ImmutableMemtable(entries, sizeInBytes.get(), maxRevision.get());
            }
            return immutableView;
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public Optional<StoredEntry> get(Slice key, long snapshotRevision) {
        return Memtable.getVisible(entries, key, snapshotRevision);
    }

    @Override
    public Iterator<InternalEntry> scan(ScanBounds bounds) {
        return Memtable.scanEntries(entries, bounds);
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    @Override
    public long entryCount() {
        return entries.size();
    }

    private static long estimatedHeapBytes(InternalEntry entry) {
        return switch (entry) {
            case InternalEntry.Value value -> ENTRY_OVERHEAD + entry.userKey().length() + value.value().length() + Long.BYTES;
            case InternalEntry.Tombstone _ -> ENTRY_OVERHEAD + entry.userKey().length() + Long.BYTES;
        };
    }

    private static StorageException.InvalidRevision invalidRevision(InternalEntry attempted, InternalEntry existing) {
        return new StorageException.InvalidRevision(
            "Conflicting mutation for key %s at revision %d"
                .formatted(attempted.userKey(), attempted.revision())
        );
    }

    enum WriteResult {
        APPLIED,
        DUPLICATE,
        FROZEN
    }
}
