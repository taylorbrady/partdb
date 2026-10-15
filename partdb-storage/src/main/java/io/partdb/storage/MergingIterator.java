package io.partdb.storage;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

final class MergingIterator implements Iterator<InternalEntry> {

    private record Head(InternalEntry entry, int source) {}

    private static final Comparator<Head> ORDER = Comparator
        .comparing((Head head) -> head.entry().key())
        .thenComparingInt(Head::source);

    private final List<Iterator<InternalEntry>> sources;
    private final PriorityQueue<Head> heap = new PriorityQueue<>(ORDER);

    MergingIterator(List<Iterator<InternalEntry>> sources) {
        this.sources = List.copyOf(sources);
        for (int i = 0; i < this.sources.size(); i++) {
            refill(i);
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public InternalEntry next() {
        Head head = heap.poll();
        if (head == null) {
            throw new NoSuchElementException();
        }
        refill(head.source());
        return head.entry();
    }

    private void refill(int source) {
        Iterator<InternalEntry> iterator = sources.get(source);
        if (iterator.hasNext()) {
            heap.offer(new Head(iterator.next(), source));
        }
    }
}
