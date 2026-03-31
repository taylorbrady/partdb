package io.partdb.storage;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

final class MergingIterator implements Iterator<StoredEntry> {

    private static final Comparator<IteratorEntry> ENTRY_ORDER = Comparator
        .comparing((IteratorEntry e) -> e.entry().key())
        .thenComparing((a, b) -> Long.compare(b.entry().revision(), a.entry().revision()))
        .thenComparingInt(e -> e.index);

    private final PriorityQueue<IteratorEntry> heap;
    private final List<Iterator<StoredEntry>> sources;
    private Slice lastEmittedKey;
    private StoredEntry nextEntry;

    public MergingIterator(List<Iterator<StoredEntry>> sources) {
        this.sources = sources;
        this.heap = new PriorityQueue<>(ENTRY_ORDER);

        for (int i = 0; i < sources.size(); i++) {
            Iterator<StoredEntry> source = sources.get(i);
            if (source.hasNext()) {
                heap.offer(new IteratorEntry(source.next(), i));
            }
        }

        this.lastEmittedKey = null;
        advance();
    }

    private void advance() {
        while (!heap.isEmpty()) {
            IteratorEntry current = heap.poll();
            Iterator<StoredEntry> source = sources.get(current.index);

            if (source.hasNext()) {
                heap.offer(new IteratorEntry(source.next(), current.index));
            }

            if (lastEmittedKey != null && current.entry().key().equals(lastEmittedKey)) {
                continue;
            }

            lastEmittedKey = current.entry().key();
            nextEntry = current.entry();
            return;
        }

        nextEntry = null;
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public StoredEntry next() {
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }
        StoredEntry result = nextEntry;
        advance();
        return result;
    }

    private record IteratorEntry(StoredEntry entry, int index) {}
}
