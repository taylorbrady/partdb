package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.ScanMode;
import io.partdb.storage.VersionedKey;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class SkipListMemtable implements Memtable {

    private static final int ENTRY_OVERHEAD = 100;

    private final ConcurrentSkipListMap<VersionedKey, Entry> entries;
    private final AtomicLong sizeInBytes;

    public SkipListMemtable() {
        this.entries = new ConcurrentSkipListMap<>();
        this.sizeInBytes = new AtomicLong(0);
    }

    @Override
    public void put(Entry entry) {
        VersionedKey key = new VersionedKey(entry.key(), entry.timestamp());
        entries.put(key, entry);
        sizeInBytes.addAndGet(estimateEntrySize(entry));
    }

    @Override
    public Optional<Entry> get(ByteArray key, Timestamp readTimestamp) {
        VersionedKey searchKey = new VersionedKey(key, readTimestamp);
        Map.Entry<VersionedKey, Entry> ceiling = entries.ceilingEntry(searchKey);

        if (ceiling == null) {
            return Optional.empty();
        }

        if (!ceiling.getKey().key().equals(key)) {
            return Optional.empty();
        }

        return Optional.of(ceiling.getValue());
    }

    @Override
    public Iterator<Entry> scan(ScanMode mode, ByteArray startKey, ByteArray endKey) {
        ConcurrentNavigableMap<VersionedKey, Entry> range;

        if (startKey == null && endKey == null) {
            range = entries;
        } else if (startKey == null) {
            VersionedKey endVersionedKey = new VersionedKey(endKey, Timestamp.MAX);
            range = entries.headMap(endVersionedKey, false);
        } else if (endKey == null) {
            VersionedKey startVersionedKey = new VersionedKey(startKey, Timestamp.MAX);
            range = entries.tailMap(startVersionedKey, true);
        } else {
            VersionedKey startVersionedKey = new VersionedKey(startKey, Timestamp.MAX);
            VersionedKey endVersionedKey = new VersionedKey(endKey, Timestamp.MAX);
            range = entries.subMap(startVersionedKey, true, endVersionedKey, false);
        }

        return switch (mode) {
            case ScanMode.Snapshot(var readTimestamp) -> new SnapshotIterator(range.values().iterator(), readTimestamp);
            case ScanMode.AllVersions() -> new AllVersionsIterator(range.values().iterator());
        };
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
            case Entry.Put value -> ENTRY_OVERHEAD + entry.key().length() + value.value().length() + 8;
            case Entry.Tombstone _ -> ENTRY_OVERHEAD + entry.key().length() + 8;
        };
    }

    private static final class SnapshotIterator implements Iterator<Entry> {

        private final Iterator<Entry> delegate;
        private final Timestamp readTimestamp;
        private ByteArray lastKey;
        private Entry next;

        SnapshotIterator(Iterator<Entry> delegate, Timestamp readTimestamp) {
            this.delegate = delegate;
            this.readTimestamp = readTimestamp;
            this.lastKey = null;
            advance();
        }

        private void advance() {
            while (delegate.hasNext()) {
                Entry entry = delegate.next();

                if (entry.timestamp().compareTo(readTimestamp) > 0) {
                    continue;
                }

                if (lastKey != null && entry.key().equals(lastKey)) {
                    continue;
                }

                lastKey = entry.key();
                next = entry;
                return;
            }
            next = null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            Entry result = next;
            advance();
            return result;
        }
    }

    private static final class AllVersionsIterator implements Iterator<Entry> {

        private final Iterator<Entry> delegate;

        AllVersionsIterator(Iterator<Entry> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Entry next() {
            return delegate.next();
        }
    }
}
