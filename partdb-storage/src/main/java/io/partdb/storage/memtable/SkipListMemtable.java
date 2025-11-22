package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.util.Iterator;
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

        if (isExpired(entry)) {
            return Optional.empty();
        }

        return Optional.of(entry);
    }

    @Override
    public Iterator<Entry> scan(ByteArray startKey, ByteArray endKey) {
        if (startKey == null && endKey == null) {
            return entries.values().iterator();
        } else if (startKey == null) {
            return entries.headMap(endKey, false).values().iterator();
        } else if (endKey == null) {
            return entries.tailMap(startKey, true).values().iterator();
        } else {
            return entries.subMap(startKey, true, endKey, false).values().iterator();
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
        long size = ENTRY_OVERHEAD;
        size += entry.key().size();
        if (entry.value() != null) {
            size += entry.value().size();
        }
        return size;
    }

    private boolean isExpired(Entry entry) {
        if (entry.expiresAtMillis() == 0) {
            return false;
        }
        return System.currentTimeMillis() > entry.expiresAtMillis();
    }
}
