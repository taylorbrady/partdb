package io.partdb.storage;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Optional;

final class ImmutableMemtable implements Memtable {

    private final NavigableMap<Slice, Mutation> entries;
    private final long sizeInBytes;

    ImmutableMemtable(NavigableMap<Slice, Mutation> entries, long sizeInBytes) {
        this.entries = entries;
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public Optional<Mutation> get(Slice key) {
        return Optional.ofNullable(entries.get(key));
    }

    @Override
    public Iterator<Mutation> scan(ScanBounds bounds) {
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
}
