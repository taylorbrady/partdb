package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

final class InternalEntryMergingIterator implements Iterator<InternalEntry> {

    private static final Comparator<IteratorEntry> ENTRY_ORDER = Comparator
        .comparing((IteratorEntry e) -> e.entry().key())
        .thenComparingInt(e -> e.index);

    private final PriorityQueue<IteratorEntry> heap;
    private final List<Iterator<InternalEntry>> sources;

    InternalEntryMergingIterator(List<Iterator<InternalEntry>> sources) {
        this.sources = sources;
        this.heap = new PriorityQueue<>(ENTRY_ORDER);

        for (int i = 0; i < sources.size(); i++) {
            Iterator<InternalEntry> source = sources.get(i);
            if (source.hasNext()) {
                heap.offer(new IteratorEntry(source.next(), i));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public InternalEntry next() {
        if (heap.isEmpty()) {
            throw new NoSuchElementException();
        }

        IteratorEntry current = heap.poll();
        Iterator<InternalEntry> source = sources.get(current.index());
        if (source.hasNext()) {
            heap.offer(new IteratorEntry(source.next(), current.index()));
        }
        return current.entry();
    }

    private record IteratorEntry(InternalEntry entry, int index) {}
}
