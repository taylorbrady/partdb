package io.partdb.storage.memtable;

import io.partdb.common.Slice;
import io.partdb.storage.Mutation;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class SkipListMemtable implements Memtable {

    private static final int ENTRY_OVERHEAD = 100;

    private final ConcurrentSkipListMap<Slice, Mutation> entries;
    private final AtomicLong sizeInBytes;

    public SkipListMemtable() {
        this.entries = new ConcurrentSkipListMap<>();
        this.sizeInBytes = new AtomicLong(0);
    }

    @Override
    public void put(Mutation mutation) {
        Mutation previous = entries.put(mutation.key(), mutation);
        long delta = estimateEntrySize(mutation);
        if (previous != null) {
            delta -= estimateEntrySize(previous);
        }
        sizeInBytes.addAndGet(delta);
    }

    @Override
    public Optional<Mutation> get(Slice key) {
        return Optional.ofNullable(entries.get(key));
    }

    @Override
    public Iterator<Mutation> scan(Slice startKey, Slice endKey) {
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

    @Override
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    @Override
    public long entryCount() {
        return entries.size();
    }

    @Override
    public void clear() {
        entries.clear();
        sizeInBytes.set(0);
    }

    private long estimateEntrySize(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Put p -> ENTRY_OVERHEAD + mutation.key().length() + p.value().length() + 8;
            case Mutation.Tombstone _ -> ENTRY_OVERHEAD + mutation.key().length() + 8;
        };
    }
}
