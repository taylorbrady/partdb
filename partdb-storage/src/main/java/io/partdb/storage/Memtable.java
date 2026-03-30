package io.partdb.storage;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

final class Memtable {

    private static final int ENTRY_OVERHEAD = 100;

    private final ConcurrentSkipListMap<Slice, Mutation> entries;
    private final AtomicLong sizeInBytes;

    Memtable() {
        this.entries = new ConcurrentSkipListMap<>();
        this.sizeInBytes = new AtomicLong(0);
    }

    WriteResult put(Mutation mutation) {
        final long[] delta = {0};
        final WriteResult[] result = new WriteResult[1];

        entries.compute(mutation.key(), (_, existing) -> {
            if (existing == null) {
                delta[0] = estimateEntrySize(mutation);
                result[0] = WriteResult.APPLIED;
                return mutation;
            }

            int revisionComparison = Long.compare(mutation.revision(), existing.revision());
            if (revisionComparison > 0) {
                delta[0] = estimateEntrySize(mutation) - estimateEntrySize(existing);
                result[0] = WriteResult.APPLIED;
                return mutation;
            }

            if (revisionComparison == 0 && existing.equals(mutation)) {
                result[0] = WriteResult.DUPLICATE;
                return existing;
            }

            throw invalidRevision(mutation, existing);
        });

        if (delta[0] != 0) {
            sizeInBytes.addAndGet(delta[0]);
        }
        return result[0];
    }

    Optional<Mutation> get(Slice key) {
        return Optional.ofNullable(entries.get(key));
    }

    Iterator<Mutation> scan(Slice startKey, Slice endKey) {
        ConcurrentNavigableMap<Slice, Mutation> range;

        if (startKey == null && endKey == null) {
            range = entries;
        } else if (startKey == null) {
            range = entries.headMap(endKey, false);
        } else if (endKey == null) {
            range = entries.tailMap(startKey, true);
        } else {
            range = entries.subMap(startKey, true, endKey, false);
        }

        return range.values().iterator();
    }

    long sizeInBytes() {
        return sizeInBytes.get();
    }

    long entryCount() {
        return entries.size();
    }

    private static long estimateEntrySize(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Put p -> ENTRY_OVERHEAD + mutation.key().length() + p.value().length() + Long.BYTES;
            case Mutation.Tombstone _ -> ENTRY_OVERHEAD + mutation.key().length() + Long.BYTES;
        };
    }

    private static StorageException.InvalidRevision invalidRevision(Mutation attempted, Mutation existing) {
        if (attempted.revision() < existing.revision()) {
            return new StorageException.InvalidRevision(
                "Revision %d for key %s is older than current revision %d"
                    .formatted(attempted.revision(), attempted.key(), existing.revision())
            );
        }

        return new StorageException.InvalidRevision(
            "Conflicting mutation for key %s at revision %d"
                .formatted(attempted.key(), attempted.revision())
        );
    }

    enum WriteResult {
        APPLIED,
        DUPLICATE
    }
}
