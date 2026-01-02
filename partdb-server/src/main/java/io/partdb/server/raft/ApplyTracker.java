package io.partdb.server.raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public final class ApplyTracker {
    private final AtomicLong lastApplied = new AtomicLong();
    private final ConcurrentNavigableMap<Long, List<CompletableFuture<Long>>> waiters =
        new ConcurrentSkipListMap<>();

    public void advance(long index) {
        lastApplied.set(index);
        var satisfied = waiters.headMap(index, true);
        for (var entry : satisfied.entrySet()) {
            long waitedIndex = entry.getKey();
            for (var future : entry.getValue()) {
                future.complete(waitedIndex);
            }
        }
        satisfied.clear();
    }

    public CompletableFuture<Long> waitFor(long index) {
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

    public long lastApplied() {
        return lastApplied.get();
    }

    public void failAll(Throwable cause) {
        for (var entry : waiters.entrySet()) {
            for (var future : entry.getValue()) {
                future.completeExceptionally(cause);
            }
        }
        waiters.clear();
    }
}
