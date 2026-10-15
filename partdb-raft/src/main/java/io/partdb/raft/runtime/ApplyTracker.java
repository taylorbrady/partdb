package io.partdb.raft.runtime;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

final class ApplyTracker {
    private final AtomicLong lastApplied = new AtomicLong();
    private final ConcurrentNavigableMap<Long, List<CompletableFuture<Long>>> waiters = new ConcurrentSkipListMap<>();

    void advance(long index) {
        lastApplied.accumulateAndGet(index, Math::max);
        var satisfied = waiters.headMap(index, true);
        satisfied.forEach((waitedIndex, futures) -> futures.forEach(future -> future.complete(waitedIndex)));
        satisfied.clear();
    }

    CompletableFuture<Long> waitFor(long index) {
        if (lastApplied.get() >= index) {
            return CompletableFuture.completedFuture(index);
        }
        var future = new CompletableFuture<Long>();
        waiters.computeIfAbsent(index, _ -> new CopyOnWriteArrayList<>()).add(future);
        if (lastApplied.get() >= index) {
            future.complete(index);
        }
        return future;
    }

    long lastApplied() {
        return lastApplied.get();
    }

    void failAll(Throwable cause) {
        waiters.values().forEach(futures -> futures.forEach(future -> future.completeExceptionally(cause)));
        waiters.clear();
    }
}
