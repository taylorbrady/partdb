package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;

import java.util.*;

public final class MergingIterator implements CloseableIterator<StoreEntry> {

    private final PriorityQueue<IteratorEntry> heap;
    private final List<CloseableIterator<StoreEntry>> iterators;
    private ByteArray lastKey;
    private StoreEntry nextEntry;

    public MergingIterator(List<CloseableIterator<StoreEntry>> iterators) {
        this.iterators = iterators;
        this.heap = new PriorityQueue<>(Comparator
            .comparing((IteratorEntry e) -> e.entry.key())
            .thenComparingInt(e -> e.iteratorIndex));

        for (int i = 0; i < iterators.size(); i++) {
            CloseableIterator<StoreEntry> it = iterators.get(i);
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
                StoreEntry next = iterators.get(current.iteratorIndex).next();
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
    public StoreEntry next() {
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }
        StoreEntry result = nextEntry;
        advance();
        return result;
    }

    @Override
    public void close() {
        for (CloseableIterator<StoreEntry> it : iterators) {
            it.close();
        }
    }

    private record IteratorEntry(StoreEntry entry, int iteratorIndex) {}
}
