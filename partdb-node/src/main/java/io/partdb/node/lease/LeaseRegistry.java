package io.partdb.node.lease;

import io.partdb.bytes.Bytes;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public final class LeaseRegistry {
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

    public void attachKey(long leaseId, Bytes key) {
        var lease = byId.get(leaseId);
        if (lease != null) {
            lease.keys().add(key);
        }
    }

    public void detachKey(long leaseId, Bytes key) {
        var lease = byId.get(leaseId);
        if (lease != null) {
            lease.keys().remove(key);
        }
    }

    public List<Bytes> attachedKeys(long leaseId) {
        var lease = byId.get(leaseId);
        if (lease == null) {
            return List.of();
        }

        return lease.keys().stream().toList();
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
        return byId.containsKey(leaseId);
    }

    public int leaseCount() {
        return byId.size();
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
        var snapshotLeases = new ArrayList<>(byId.values());
        int size = 4 + snapshotLeases.size() * 16;
        var buffer = ByteBuffer.allocate(size);

        buffer.putInt(snapshotLeases.size());
        for (var lease : snapshotLeases) {
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
