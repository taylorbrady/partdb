package io.partdb.storage;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Optional;

final class ImmutableMemtable implements Memtable {

    private final NavigableMap<InternalKey, InternalEntry> entries;
    private final long sizeInBytes;
    private final long maxRevision;

    ImmutableMemtable(NavigableMap<InternalKey, InternalEntry> entries, long sizeInBytes, long maxRevision) {
        this.entries = java.util.Collections.unmodifiableNavigableMap(entries);
        this.sizeInBytes = sizeInBytes;
        this.maxRevision = maxRevision;
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
        return sizeInBytes;
    }

    @Override
    public long entryCount() {
        return entries.size();
    }

    long maxRevision() {
        return maxRevision;
    }
}
