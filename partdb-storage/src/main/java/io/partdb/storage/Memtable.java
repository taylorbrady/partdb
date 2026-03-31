package io.partdb.storage;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Optional;

sealed interface Memtable permits MutableMemtable, ImmutableMemtable {

    Optional<StoredEntry> get(Slice key);

    Iterator<StoredEntry> scan(ScanBounds bounds);

    long sizeInBytes();

    long entryCount();

    static Iterator<StoredEntry> scanEntries(NavigableMap<Slice, StoredEntry> entries, ScanBounds bounds) {
        NavigableMap<Slice, StoredEntry> range;
        Slice startKey = bounds.startInclusive();
        Slice endKey = bounds.endExclusive();

        if (bounds.isAll()) {
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
}
