package io.partdb.storage;

import io.partdb.common.Slice;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public final class MergingIterator implements Iterator<Mutation> {

    private final PriorityQueue<IteratorEntry> heap;
    private final List<Iterator<Mutation>> iterators;
    private Slice lastKey;
    private Mutation nextMutation;

    public MergingIterator(List<Iterator<Mutation>> iterators) {
        this.iterators = iterators;
        this.heap = new PriorityQueue<>(Comparator
            .comparing((IteratorEntry e) -> e.mutation.key())
            .thenComparingLong((IteratorEntry e) -> -e.mutation.revision())
            .thenComparingInt(e -> e.iteratorIndex));

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Mutation> it = iterators.get(i);
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
                Mutation next = iterators.get(current.iteratorIndex).next();
                heap.offer(new IteratorEntry(next, current.iteratorIndex));
            }

            if (lastKey != null && current.mutation.key().equals(lastKey)) {
                continue;
            }

            lastKey = current.mutation.key();
            nextMutation = current.mutation;
            return;
        }

        nextMutation = null;
    }

    @Override
    public boolean hasNext() {
        return nextMutation != null;
    }

    @Override
    public Mutation next() {
        if (nextMutation == null) {
            throw new NoSuchElementException();
        }
        Mutation result = nextMutation;
        advance();
        return result;
    }

    private record IteratorEntry(Mutation mutation, int iteratorIndex) {}
}
