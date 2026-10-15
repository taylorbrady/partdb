package io.partdb.storage;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

final class Memtable {

    private static final int ENTRY_OVERHEAD = 64;

    private final ConcurrentSkipListMap<InternalKey, InternalEntry> entries = new ConcurrentSkipListMap<>();
    private volatile long sizeBytes;
    private volatile long appliedIndex;

    Memtable(long appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    void put(InternalEntry entry) {
        entries.put(entry.key(), entry);
        sizeBytes += ENTRY_OVERHEAD + entry.encodedSize();
    }

    void advanceTo(long index) {
        appliedIndex = index;
    }

    long appliedIndex() {
        return appliedIndex;
    }

    long sizeBytes() {
        return sizeBytes;
    }

    boolean isEmpty() {
        return entries.isEmpty();
    }

    Optional<InternalEntry> get(Slice key, long index) {
        Map.Entry<InternalKey, InternalEntry> candidate = entries.ceilingEntry(new InternalKey(key, index));
        return candidate != null && candidate.getKey().userKey().equals(key)
            ? Optional.of(candidate.getValue())
            : Optional.empty();
    }

    Iterator<InternalEntry> scan(Bounds bounds) {
        NavigableMap<InternalKey, InternalEntry> range = entries;
        if (bounds.start() != null) {
            range = range.tailMap(InternalKey.first(bounds.start()), true);
        }
        if (bounds.end() != null) {
            range = range.headMap(InternalKey.first(bounds.end()), false);
        }
        return range.values().iterator();
    }
}
