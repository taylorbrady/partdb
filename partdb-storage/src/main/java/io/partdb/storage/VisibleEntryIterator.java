package io.partdb.storage;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

final class VisibleEntryIterator implements Iterator<StoredEntry.Value> {

    private static final Comparator<IteratorEntry> ENTRY_ORDER = Comparator
        .comparing((IteratorEntry e) -> e.entry().key())
        .thenComparingInt(e -> e.index);

    private final PriorityQueue<IteratorEntry> heap;
    private final List<Iterator<InternalEntry>> sources;
    private final long snapshotRevision;
    private Slice lastEmittedKey;
    private StoredEntry.Value nextEntry;

    VisibleEntryIterator(List<Iterator<InternalEntry>> sources, long snapshotRevision) {
        this.sources = sources;
        this.snapshotRevision = snapshotRevision;
        this.heap = new PriorityQueue<>(ENTRY_ORDER);

        for (int i = 0; i < sources.size(); i++) {
            Iterator<InternalEntry> source = sources.get(i);
            if (source.hasNext()) {
                heap.offer(new IteratorEntry(source.next(), i));
            }
        }

        this.lastEmittedKey = null;
        advance();
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public StoredEntry.Value next() {
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }
        StoredEntry.Value result = nextEntry;
        advance();
        return result;
    }

    private void advance() {
        while (!heap.isEmpty()) {
            IteratorEntry current = heap.poll();
            Iterator<InternalEntry> source = sources.get(current.index);

            if (source.hasNext()) {
                heap.offer(new IteratorEntry(source.next(), current.index));
            }

            if (lastEmittedKey != null && current.entry.userKey().equals(lastEmittedKey)) {
                continue;
            }

            if (current.entry.revision() > snapshotRevision) {
                continue;
            }

            lastEmittedKey = current.entry.userKey();
            if (current.entry instanceof InternalEntry.Value value) {
                nextEntry = value.toStoredEntry();
                return;
            }
        }

        nextEntry = null;
    }

    private record IteratorEntry(InternalEntry entry, int index) {}
}
