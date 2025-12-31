package io.partdb.server.raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public final class ApplyTracker {
    private final AtomicLong lastApplied = new AtomicLong();
    private final ConcurrentNavigableMap<Long, List<CompletableFuture<Void>>> waiters =
        new ConcurrentSkipListMap<>();

    public void advance(long index) {
        lastApplied.set(index);
        var satisfied = waiters.headMap(index, true);
        for (var futures : satisfied.values()) {
            for (var future : futures) {
                future.complete(null);
            }
        }
        satisfied.clear();
    }

    public CompletableFuture<Void> waitFor(long index) {
        if (lastApplied.get() >= index) {
            return CompletableFuture.completedFuture(null);
        }
        var future = new CompletableFuture<Void>();
        waiters.computeIfAbsent(index, _ -> new CopyOnWriteArrayList<>()).add(future);
        if (lastApplied.get() >= index) {
            future.complete(null);
        }
        return future;
    }

    public long lastApplied() {
        return lastApplied.get();
    }
}
