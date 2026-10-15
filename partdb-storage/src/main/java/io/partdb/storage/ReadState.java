package io.partdb.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

final class ReadState {

    private final long index;
    private final Memtable active;
    private final List<Memtable> frozen;
    private final Version version;
    private final AtomicInteger refs = new AtomicInteger(1);

    ReadState(long index, Memtable active, List<Memtable> frozen, Version version) {
        this.index = index;
        this.active = active;
        this.frozen = List.copyOf(frozen);
        this.version = version;
    }

    long index() {
        return index;
    }

    Memtable active() {
        return active;
    }

    List<Memtable> frozen() {
        return frozen;
    }

    Version version() {
        return version;
    }

    boolean tryRetain() {
        int current;
        do {
            current = refs.get();
            if (current == 0) {
                return false;
            }
        } while (!refs.compareAndSet(current, current + 1));
        return true;
    }

    void release() {
        int remaining = refs.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("Read state released too many times");
        }
        if (remaining == 0) {
            version.release();
        }
    }

    ReadState advancedTo(long index) {
        return new ReadState(index, active, frozen, version.retain());
    }

    ReadState rotated() {
        List<Memtable> next = new ArrayList<>(frozen.size() + 1);
        next.add(active);
        next.addAll(frozen);
        return new ReadState(index, new Memtable(index), next, version.retain());
    }

    ReadState flushed(Memtable memtable, Version next) {
        if (frozen.isEmpty() || frozen.getLast() != memtable) {
            throw new IllegalStateException("Flushed memtable is not the oldest frozen memtable");
        }
        return new ReadState(index, active, frozen.subList(0, frozen.size() - 1), next);
    }

    ReadState withVersion(Version next) {
        return new ReadState(index, active, frozen, next);
    }

    Optional<InternalEntry> find(Slice key) {
        Optional<InternalEntry> found = active.get(key, index);
        for (int i = 0; found.isEmpty() && i < frozen.size(); i++) {
            found = frozen.get(i).get(key, index);
        }
        return found.isPresent() ? found : version.get(key, index);
    }

    Iterator<InternalEntry> scan(Bounds bounds) {
        List<Iterator<InternalEntry>> sources = new ArrayList<>();
        sources.add(active.scan(bounds));
        for (Memtable memtable : frozen) {
            sources.add(memtable.scan(bounds));
        }
        version.addScans(sources, bounds);
        return new LatestIterator(new MergingIterator(sources), index, true);
    }
}
