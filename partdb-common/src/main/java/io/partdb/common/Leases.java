package io.partdb.common;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public final class Leases {
    private final ConcurrentHashMap<Long, Lease> byId = new ConcurrentHashMap<>();
    private final DelayQueue<LeaseExpiry> expirationQueue = new DelayQueue<>();

    public record LeaseExpiry(long leaseId, long expiresAtNanos) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expiresAtNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }
    }

    public void grant(long leaseId, long ttlNanos) {
        var lease = new Lease(leaseId, ttlNanos);
        byId.put(leaseId, lease);
        expirationQueue.offer(new LeaseExpiry(leaseId, lease.expiresAtNanos()));
    }

    public void revoke(long leaseId) {
        byId.remove(leaseId);
    }

    public void attachKey(long leaseId, byte[] key) {
        var lease = byId.get(leaseId);
        if (lease != null) {
            lease.keys().add(new KeyBytes(key));
        }
    }

    public void detachKey(long leaseId, byte[] key) {
        var lease = byId.get(leaseId);
        if (lease != null) {
            lease.keys().remove(new KeyBytes(key));
        }
    }

    public Set<KeyBytes> getKeys(long leaseId) {
        var lease = byId.get(leaseId);
        return lease != null ? Collections.unmodifiableSet(lease.keys()) : Set.of();
    }

    public void keepAlive(long leaseId) {
        var lease = byId.get(leaseId);
        if (lease != null) {
            lease.refresh();
            expirationQueue.offer(new LeaseExpiry(leaseId, lease.expiresAtNanos()));
        }
    }

    public boolean isLeaseActive(long leaseId) {
        if (leaseId == 0) {
            return true;
        }
        var lease = byId.get(leaseId);
        return lease != null && !lease.isExpired();
    }

    public LeaseExpiry pollExpired(Duration timeout) throws InterruptedException {
        return expirationQueue.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public LeaseExpiry pollExpiredNow() {
        return expirationQueue.poll();
    }

    public boolean isStale(LeaseExpiry entry) {
        var lease = byId.get(entry.leaseId());
        return lease == null || lease.expiresAtNanos() != entry.expiresAtNanos();
    }

    public byte[] toSnapshot() {
        var leases = new ArrayList<>(byId.values());
        int size = 4 + leases.size() * 16;
        var buffer = ByteBuffer.allocate(size);

        buffer.putInt(leases.size());
        for (var lease : leases) {
            buffer.putLong(lease.id());
            buffer.putLong(lease.remainingNanos());
        }

        return buffer.array();
    }

    public void restoreSnapshot(byte[] data) {
        byId.clear();
        expirationQueue.clear();

        var buffer = ByteBuffer.wrap(data);
        int count = buffer.getInt();

        for (int i = 0; i < count; i++) {
            long id = buffer.getLong();
            long remainingNanos = buffer.getLong();
            long expiresAtNanos = System.nanoTime() + remainingNanos;
            var lease = new Lease(id, remainingNanos, expiresAtNanos);
            byId.put(id, lease);
            expirationQueue.offer(new LeaseExpiry(id, expiresAtNanos));
        }
    }

    public void clear() {
        byId.clear();
        expirationQueue.clear();
    }
}
