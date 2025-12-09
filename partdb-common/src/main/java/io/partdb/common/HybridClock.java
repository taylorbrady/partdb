package io.partdb.common;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public final class HybridClock {

    private final AtomicLong state;
    private final LongSupplier wallTimeSupplier;
    private final boolean mutable;

    private HybridClock(long initialState, LongSupplier wallTimeSupplier, boolean mutable) {
        this.state = new AtomicLong(initialState);
        this.wallTimeSupplier = wallTimeSupplier;
        this.mutable = mutable;
    }

    public static HybridClock system() {
        long now = System.currentTimeMillis();
        return new HybridClock(Timestamp.of(now, 0).value(), System::currentTimeMillis, true);
    }

    public static HybridClock fixed(Timestamp timestamp) {
        return new HybridClock(timestamp.value(), () -> timestamp.wallTime(), false);
    }

    public static HybridClock manual(Timestamp initial) {
        return new HybridClock(initial.value(), () -> -1, true);
    }

    public Timestamp tick() {
        if (!mutable) {
            return new Timestamp(state.get());
        }

        long wallNow = wallTimeSupplier.getAsLong();

        while (true) {
            long currentState = state.get();
            Timestamp current = new Timestamp(currentState);

            Timestamp next;
            if (wallNow > current.wallTime()) {
                next = Timestamp.of(wallNow, 0);
            } else {
                int nextLogical = current.logical() + 1;
                if (nextLogical > 0xFFFF) {
                    next = Timestamp.of(current.wallTime() + 1, 0);
                } else {
                    next = Timestamp.of(current.wallTime(), nextLogical);
                }
            }

            if (state.compareAndSet(currentState, next.value())) {
                return next;
            }
        }
    }

    public Timestamp current() {
        return new Timestamp(state.get());
    }

    public Timestamp receive(Timestamp remote) {
        if (!mutable) {
            return new Timestamp(state.get());
        }

        long wallNow = wallTimeSupplier.getAsLong();

        while (true) {
            long currentState = state.get();
            Timestamp current = new Timestamp(currentState);

            long maxWall = Math.max(wallNow, Math.max(current.wallTime(), remote.wallTime()));

            int nextLogical;
            if (maxWall > current.wallTime() && maxWall > remote.wallTime()) {
                nextLogical = 0;
            } else if (maxWall == current.wallTime() && maxWall == remote.wallTime()) {
                nextLogical = Math.max(current.logical(), remote.logical()) + 1;
            } else if (maxWall == current.wallTime()) {
                nextLogical = current.logical() + 1;
            } else {
                nextLogical = remote.logical() + 1;
            }

            if (nextLogical > 0xFFFF) {
                maxWall++;
                nextLogical = 0;
            }

            Timestamp next = Timestamp.of(maxWall, nextLogical);

            if (state.compareAndSet(currentState, next.value())) {
                return next;
            }
        }
    }

    public void advance(Timestamp to) {
        if (!mutable) {
            throw new UnsupportedOperationException("Cannot advance a fixed clock");
        }

        while (true) {
            long currentState = state.get();
            if (to.value() <= currentState) {
                return;
            }
            if (state.compareAndSet(currentState, to.value())) {
                return;
            }
        }
    }
}
