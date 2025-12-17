package io.partdb.storage;

import io.partdb.common.Slice;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public final class MergingIterator implements Iterator<Mutation> {

    private static final Comparator<IteratorEntry> ENTRY_ORDER = Comparator
        .comparing((IteratorEntry e) -> e.mutation.key())
        .thenComparing((a, b) -> Long.compare(b.mutation.revision(), a.mutation.revision()))
        .thenComparingInt(e -> e.index);

    private final PriorityQueue<IteratorEntry> heap;
    private final List<Iterator<Mutation>> sources;
    private Slice lastEmittedKey;
    private Mutation nextMutation;

    public MergingIterator(List<Iterator<Mutation>> sources) {
        this.sources = sources;
        this.heap = new PriorityQueue<>(ENTRY_ORDER);

        for (int i = 0; i < sources.size(); i++) {
            Iterator<Mutation> source = sources.get(i);
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
            Iterator<Mutation> source = sources.get(current.index);

            if (source.hasNext()) {
                heap.offer(new IteratorEntry(source.next(), current.index));
            }

            if (lastEmittedKey != null && current.mutation.key().equals(lastEmittedKey)) {
                continue;
            }

            lastEmittedKey = current.mutation.key();
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

    private record IteratorEntry(Mutation mutation, int index) {}
}
