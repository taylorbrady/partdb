package io.partdb.raft;

import java.util.ArrayDeque;

final class Progress {

    enum State { PROBE, REPLICATE, SNAPSHOT }

    private final int maxInflight;
    private final ArrayDeque<Long> inflight = new ArrayDeque<>();
    private State state = State.PROBE;
    private long match;
    private long next;
    private boolean probeSent;
    private long pendingSnapshot;
    private boolean recentActive;

    Progress(long next, int maxInflight) {
        this.next = next;
        this.maxInflight = maxInflight;
    }

    State state() {
        return state;
    }

    long match() {
        return match;
    }

    long next() {
        return next;
    }

    long pendingSnapshot() {
        return pendingSnapshot;
    }

    boolean recentActive() {
        return recentActive;
    }

    void markActive() {
        recentActive = true;
    }

    void resetActive() {
        recentActive = false;
    }

    boolean isPaused() {
        return switch (state) {
            case PROBE -> probeSent;
            case REPLICATE -> inflight.size() >= maxInflight;
            case SNAPSHOT -> true;
        };
    }

    void resume() {
        probeSent = false;
    }

    boolean maybeUpdate(long index) {
        boolean updated = index > match;
        if (updated) {
            match = index;
            probeSent = false;
        }
        next = Math.max(next, index + 1);
        return updated;
    }

    boolean maybeDecrease(long rejected, long hint) {
        if (state == State.REPLICATE) {
            if (rejected <= match) {
                return false;
            }
            next = match + 1;
            return true;
        }
        if (next - 1 != rejected) {
            return false;
        }
        next = Math.max(Math.min(rejected, hint + 1), match + 1);
        probeSent = false;
        return true;
    }

    void sent(long lastIndex, boolean hasEntries) {
        switch (state) {
            case REPLICATE -> {
                if (hasEntries) {
                    next = lastIndex + 1;
                    inflight.addLast(lastIndex);
                }
            }
            case PROBE -> probeSent = true;
            case SNAPSHOT -> throw new IllegalStateException("cannot send entries while a snapshot is pending");
        }
    }

    void acknowledged(long index) {
        while (!inflight.isEmpty() && inflight.peekFirst() <= index) {
            inflight.pollFirst();
        }
    }

    void becomeProbe() {
        next = state == State.SNAPSHOT ? Math.max(match + 1, pendingSnapshot + 1) : match + 1;
        state = State.PROBE;
        probeSent = false;
        pendingSnapshot = 0;
        inflight.clear();
    }

    void becomeReplicate() {
        state = State.REPLICATE;
        next = match + 1;
        probeSent = false;
        inflight.clear();
    }

    void becomeSnapshot(long snapshotIndex) {
        state = State.SNAPSHOT;
        pendingSnapshot = snapshotIndex;
        probeSent = false;
        inflight.clear();
    }

    void pauseProbe() {
        probeSent = true;
    }

    void abortSnapshot() {
        pendingSnapshot = 0;
        becomeProbe();
    }
}
