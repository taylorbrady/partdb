package io.partdb.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class Leases {

    private final Map<Long, Lease> active = new ConcurrentHashMap<>();
    private final Map<Long, Set<ByteArray>> leaseKeys = new ConcurrentHashMap<>();

    public void grant(long leaseId, long ttlMillis, long grantedAtMillis) {
        active.put(leaseId, new Lease(leaseId, ttlMillis, grantedAtMillis));
        leaseKeys.put(leaseId, ConcurrentHashMap.newKeySet());
    }

    public void revoke(long leaseId) {
        active.remove(leaseId);
        leaseKeys.remove(leaseId);
    }

    public void attachKey(long leaseId, ByteArray key) {
        Set<ByteArray> keys = leaseKeys.get(leaseId);
        if (keys != null) {
            keys.add(key);
        }
    }

    public void detachKey(long leaseId, ByteArray key) {
        Set<ByteArray> keys = leaseKeys.get(leaseId);
        if (keys != null) {
            keys.remove(key);
        }
    }

    public Set<ByteArray> getKeys(long leaseId) {
        Set<ByteArray> keys = leaseKeys.get(leaseId);
        return keys != null ? Collections.unmodifiableSet(keys) : Set.of();
    }

    public void keepAlive(long leaseId, long currentTimeMillis) {
        active.computeIfPresent(leaseId, (id, lease) -> lease.renew(currentTimeMillis));
    }

    public boolean isLeaseActive(long leaseId) {
        if (leaseId == 0) {
            return true;
        }
        Lease lease = active.get(leaseId);
        return lease != null && !lease.isExpired(System.currentTimeMillis());
    }

    public List<Long> getExpired(long currentTimeMillis) {
        List<Long> expired = new ArrayList<>();
        for (Map.Entry<Long, Lease> entry : active.entrySet()) {
            if (entry.getValue().isExpired(currentTimeMillis)) {
                expired.add(entry.getKey());
            }
        }
        return expired;
    }

    public byte[] toSnapshot() {
        List<Lease> snapshot = new ArrayList<>(active.values());
        int size = 4 + snapshot.size() * 24;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(snapshot.size());
        for (Lease lease : snapshot) {
            buffer.putLong(lease.id());
            buffer.putLong(lease.ttlMillis());
            buffer.putLong(lease.grantedAtMillis());
        }

        return buffer.array();
    }

    public void restoreSnapshot(byte[] data) {
        active.clear();
        leaseKeys.clear();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int count = buffer.getInt();

        for (int i = 0; i < count; i++) {
            long id = buffer.getLong();
            long ttl = buffer.getLong();
            long granted = buffer.getLong();
            active.put(id, new Lease(id, ttl, granted));
            leaseKeys.put(id, ConcurrentHashMap.newKeySet());
        }
    }

    public void clear() {
        active.clear();
        leaseKeys.clear();
    }
}
