package io.partdb.storage;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Optional;

sealed interface Memtable permits MutableMemtable, ImmutableMemtable {

    Optional<StoredEntry> get(Slice key, long snapshotRevision);

    Iterator<InternalEntry> scan(ScanBounds bounds);

    long sizeInBytes();

    long entryCount();

    static Optional<StoredEntry> getVisible(
        NavigableMap<InternalKey, InternalEntry> entries,
        Slice key,
        long snapshotRevision
    ) {
        var candidate = entries.ceilingEntry(InternalKey.visibleAt(key, snapshotRevision));
        if (candidate == null || !candidate.getKey().userKey().equals(key)) {
            return Optional.empty();
        }
        return Optional.of(candidate.getValue().toStoredEntry());
    }

    static Iterator<InternalEntry> scanEntries(NavigableMap<InternalKey, InternalEntry> entries, ScanBounds bounds) {
        NavigableMap<InternalKey, InternalEntry> range;
        Slice startKey = bounds.startInclusive();
        Slice endKey = bounds.endExclusive();

        if (bounds.isAll()) {
            range = entries;
        } else if (startKey == null) {
            range = entries.headMap(InternalKey.firstForUser(endKey), false);
        } else if (endKey == null) {
            range = entries.tailMap(InternalKey.firstForUser(startKey), true);
        } else {
            range = entries.subMap(InternalKey.firstForUser(startKey), true, InternalKey.firstForUser(endKey), false);
        }

        return range.values().iterator();
    }
}
