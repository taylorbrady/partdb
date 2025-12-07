package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.storage.Entry;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class SkipListMemtable implements Memtable {

    private static final int ENTRY_OVERHEAD = 100;

    private final MemtableConfig config;
    private final ConcurrentSkipListMap<ByteArray, Entry> entries;
    private final AtomicLong sizeInBytes;

    public SkipListMemtable(MemtableConfig config) {
        this.config = config;
        this.entries = new ConcurrentSkipListMap<>();
        this.sizeInBytes = new AtomicLong(0);
    }

    @Override
    public void put(Entry entry) {
        Entry previous = entries.put(entry.key(), entry);

        long sizeDelta = estimateEntrySize(entry);
        if (previous != null) {
            sizeDelta -= estimateEntrySize(previous);
        }

        sizeInBytes.addAndGet(sizeDelta);
    }

    @Override
    public Optional<Entry> get(ByteArray key) {
        Entry entry = entries.get(key);

        if (entry == null) {
            return Optional.empty();
        }

        return Optional.of(entry);
    }

    @Override
    public CloseableIterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        if (startKey == null && endKey == null) {
            return CloseableIterator.wrap(entries.values().iterator());
        } else if (startKey == null) {
            return CloseableIterator.wrap(entries.headMap(endKey, false).values().iterator());
        } else if (endKey == null) {
            return CloseableIterator.wrap(entries.tailMap(startKey, true).values().iterator());
        } else {
            return CloseableIterator.wrap(entries.subMap(startKey, true, endKey, false).values().iterator());
        }
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

    private long estimateEntrySize(Entry entry) {
        return switch (entry) {
            case Entry.Data data -> ENTRY_OVERHEAD + entry.key().size() + data.value().size();
            case Entry.Tombstone _ -> ENTRY_OVERHEAD + entry.key().size();
        };
    }
}
