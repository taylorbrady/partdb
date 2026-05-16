package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.concurrent.atomic.AtomicLong;

final class RevisionState {

    private final AtomicLong appliedThroughRevision;
    private final AtomicLong durableThroughRevision;

    RevisionState(long durableThroughRevision) {
        if (durableThroughRevision < 0) {
            throw new IllegalArgumentException("durableThroughRevision must be non-negative");
        }
        this.appliedThroughRevision = new AtomicLong(durableThroughRevision);
        this.durableThroughRevision = new AtomicLong(durableThroughRevision);
    }

    long appliedThroughRevision() {
        return appliedThroughRevision.get();
    }

    long durableThroughRevision() {
        return durableThroughRevision.get();
    }

    void recordApplied(long revision) {
        appliedThroughRevision.accumulateAndGet(revision, Math::max);
    }

    void recordDurable(long revision) {
        durableThroughRevision.accumulateAndGet(revision, Math::max);
    }

    void restore(long durableThroughRevision) {
        if (durableThroughRevision < 0) {
            throw new IllegalArgumentException("durableThroughRevision must be non-negative");
        }
        this.appliedThroughRevision.set(durableThroughRevision);
        this.durableThroughRevision.set(durableThroughRevision);
    }
}
