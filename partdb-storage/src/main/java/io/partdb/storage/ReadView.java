package io.partdb.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

final class ReadView implements AutoCloseable {

    private final Memtable activeMemtable;
    private final List<ImmutableMemtable> immutableMemtables;
    private final VersionLease tableSnapshot;
    private final long snapshotRevision;
    private boolean closed;

    ReadView(MemtableView memtableView, VersionLease tableSnapshot, long snapshotRevision) {
        Objects.requireNonNull(memtableView, "memtableView must not be null");
        this.activeMemtable = memtableView.activeMemtable();
        this.immutableMemtables = memtableView.immutableMemtables();
        this.tableSnapshot = Objects.requireNonNull(tableSnapshot, "tableSnapshot must not be null");
        this.snapshotRevision = snapshotRevision;
        this.closed = false;
    }

    Optional<StoredEntry.Value> get(Slice key) {
        return lookupLatestEntry(key).flatMap(ReadView::resolveValue);
    }

    Optional<StoredEntry> lookupLatestEntry(Slice key) {
        Optional<StoredEntry> memtableResult = lookupStoredEntry(key, activeMemtable, immutableMemtables, snapshotRevision);
        if (memtableResult.isPresent()) {
            return memtableResult;
        }
        return tableSnapshot.get(key, snapshotRevision);
    }

    CloseableIterator<StoredEntry.Value> scan(ScanBounds bounds) {
        List<Iterator<InternalEntry>> iterators = new ArrayList<>();
        iterators.add(activeMemtable.scan(bounds));
        addImmutableMemtableIterators(iterators, bounds, immutableMemtables);

        for (SSTableReader sstable : tableSnapshot.scanTables(bounds)) {
            iterators.add(sstable.scan(bounds));
        }

        return new ScanCursor(this, new VisibleEntryIterator(iterators, snapshotRevision));
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            tableSnapshot.close();
        }
    }

    static Optional<StoredEntry> lookupStoredEntry(
        Slice key,
        Memtable activeMemtable,
        List<ImmutableMemtable> immutableMemtables,
        long snapshotRevision
    ) {
        Optional<StoredEntry> result = activeMemtable.get(key, snapshotRevision);
        if (result.isPresent()) {
            return result;
        }

        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            result = immutableMemtables.get(i).get(key, snapshotRevision);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    static Optional<StoredEntry> lookupStoredEntry(
        Slice key,
        Memtable activeMemtable,
        List<ImmutableMemtable> immutableMemtables
    ) {
        return lookupStoredEntry(key, activeMemtable, immutableMemtables, Long.MAX_VALUE);
    }

    private static Optional<StoredEntry.Value> resolveValue(StoredEntry entry) {
        return switch (entry) {
            case StoredEntry.Tombstone _ -> Optional.empty();
            case StoredEntry.Value value -> Optional.of(value);
        };
    }

    private static void addImmutableMemtableIterators(
        List<Iterator<InternalEntry>> iterators,
        ScanBounds bounds,
        List<ImmutableMemtable> immutableMemtables
    ) {
        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            iterators.add(immutableMemtables.get(i).scan(bounds));
        }
    }

    private static final class ScanCursor implements CloseableIterator<StoredEntry.Value> {
        private final ReadView readView;
        private final VisibleEntryIterator merged;
        private StoredEntry.Value next;
        private boolean closed;

        private ScanCursor(ReadView readView, VisibleEntryIterator merged) {
            this.readView = readView;
            this.merged = merged;
            this.next = advance();
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public StoredEntry.Value next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            StoredEntry.Value result = next;
            next = advance();
            return result;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                readView.close();
            }
        }

        private StoredEntry.Value advance() {
            return merged.hasNext() ? merged.next() : null;
        }
    }
}
