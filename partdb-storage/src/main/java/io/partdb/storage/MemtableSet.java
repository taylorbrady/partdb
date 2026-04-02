package io.partdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

final class MemtableSet {

    private final AtomicReference<MutableMemtable> activeMemtable;
    private final ReentrantLock stateLock;

    private volatile List<ImmutableMemtable> immutableMemtables;

    MemtableSet() {
        this.activeMemtable = new AtomicReference<>(new MutableMemtable());
        this.stateLock = new ReentrantLock();
        this.immutableMemtables = List.of();
    }

    MutableMemtable active() {
        return activeMemtable.get();
    }

    MemtableView captureView() {
        stateLock.lock();
        try {
            return new MemtableView(activeMemtable.get(), immutableMemtables);
        } finally {
            stateLock.unlock();
        }
    }

    ImmutableMemtable rotate(MutableMemtable expectedActive) {
        Objects.requireNonNull(expectedActive, "expectedActive must not be null");

        stateLock.lock();
        try {
            MutableMemtable current = activeMemtable.get();
            if (current != expectedActive) {
                return null;
            }

            ImmutableMemtable frozen = current.freeze();
            List<ImmutableMemtable> updated = new ArrayList<>(immutableMemtables);
            updated.add(frozen);
            immutableMemtables = List.copyOf(updated);
            activeMemtable.set(new MutableMemtable());
            return frozen;
        } finally {
            stateLock.unlock();
        }
    }

    void retire(ImmutableMemtable memtable) {
        Objects.requireNonNull(memtable, "memtable must not be null");

        stateLock.lock();
        try {
            if (immutableMemtables.isEmpty() || immutableMemtables.getFirst() != memtable) {
                throw new IllegalStateException("Flush completed for a memtable that is not at the queue head");
            }

            List<ImmutableMemtable> updated = new ArrayList<>(immutableMemtables);
            updated.removeFirst();
            immutableMemtables = List.copyOf(updated);
        } finally {
            stateLock.unlock();
        }
    }

    List<ImmutableMemtable> immutableMemtables() {
        return immutableMemtables;
    }

    void reset() {
        stateLock.lock();
        try {
            activeMemtable.set(new MutableMemtable());
            immutableMemtables = List.of();
        } finally {
            stateLock.unlock();
        }
    }

    long activeSizeInBytes() {
        return activeMemtable.get().sizeInBytes();
    }

    int immutableCount() {
        return immutableMemtables.size();
    }
}
