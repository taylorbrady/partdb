package io.partdb.server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class PendingRequests {
    private final AtomicLong nextId = new AtomicLong(1);
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pending = new ConcurrentHashMap<>();

    public record Tracked(long requestId, CompletableFuture<Void> future) {}

    public Tracked track() {
        long id = nextId.getAndIncrement();
        var future = new CompletableFuture<Void>();
        pending.put(id, future);
        future.whenComplete((_, _) -> pending.remove(id, future));
        return new Tracked(id, future);
    }

    public void complete(long requestId) {
        var future = pending.remove(requestId);
        if (future != null) {
            future.complete(null);
        }
    }

    public void cancel(long requestId) {
        pending.remove(requestId);
    }

    public int size() {
        return pending.size();
    }
}
