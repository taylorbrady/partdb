package io.partdb.storage;

import io.partdb.common.ByteArray;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public final class MergingIterator implements Iterator<Entry> {

    private final PriorityQueue<IteratorEntry> heap;
    private final List<Iterator<Entry>> iterators;
    private ByteArray lastKey;
    private Entry nextEntry;

    public MergingIterator(List<Iterator<Entry>> iterators) {
        this.iterators = iterators;
        this.heap = new PriorityQueue<>(Comparator
            .comparing((IteratorEntry e) -> e.entry.key())
            .thenComparing((IteratorEntry e) -> e.entry.timestamp(), Comparator.reverseOrder())
            .thenComparingInt(e -> e.iteratorIndex));

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Entry> it = iterators.get(i);
            if (it.hasNext()) {
                heap.offer(new IteratorEntry(it.next(), i));
            }
        }

        this.lastKey = null;
        advance();
    }

    private void advance() {
        while (!heap.isEmpty()) {
            IteratorEntry current = heap.poll();

            if (iterators.get(current.iteratorIndex).hasNext()) {
                Entry next = iterators.get(current.iteratorIndex).next();
                heap.offer(new IteratorEntry(next, current.iteratorIndex));
            }

            if (lastKey != null && current.entry.key().equals(lastKey)) {
                continue;
            }

            lastKey = current.entry.key();
            nextEntry = current.entry;
            return;
        }

        nextEntry = null;
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public Entry next() {
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }
        Entry result = nextEntry;
        advance();
        return result;
    }

    private record IteratorEntry(Entry entry, int iteratorIndex) {}
}
