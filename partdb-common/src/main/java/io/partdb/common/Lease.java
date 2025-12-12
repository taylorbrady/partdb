package io.partdb.common;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class Lease {
    private final long id;
    private final long ttlNanos;
    private final Set<Slice> keys = ConcurrentHashMap.newKeySet();
    private volatile long expiresAtNanos;

    public Lease(long id, long ttlNanos) {
        if (id <= 0) {
            throw new IllegalArgumentException("id must be positive");
        }
        if (ttlNanos <= 0) {
            throw new IllegalArgumentException("ttlNanos must be positive");
        }
        this.id = id;
        this.ttlNanos = ttlNanos;
        this.expiresAtNanos = System.nanoTime() + ttlNanos;
    }

    Lease(long id, long ttlNanos, long expiresAtNanos) {
        this.id = id;
        this.ttlNanos = ttlNanos;
        this.expiresAtNanos = expiresAtNanos;
    }

    public long id() {
        return id;
    }

    public long ttlNanos() {
        return ttlNanos;
    }

    public long expiresAtNanos() {
        return expiresAtNanos;
    }

    public Set<Slice> keys() {
        return keys;
    }

    public boolean isExpired() {
        return System.nanoTime() >= expiresAtNanos;
    }

    public void refresh() {
        this.expiresAtNanos = System.nanoTime() + ttlNanos;
    }

    public long remainingNanos() {
        return Math.max(0, expiresAtNanos - System.nanoTime());
    }
}
