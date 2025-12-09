package io.partdb.storage;

import io.partdb.common.Timestamp;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

final class SnapshotRegistry {

    private final ConcurrentSkipListMap<Long, AtomicInteger> activeSnapshots = new ConcurrentSkipListMap<>();

    void register(Timestamp timestamp) {
        activeSnapshots.compute(timestamp.value(), (_, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            }
            v.incrementAndGet();
            return v;
        });
    }

    void release(Timestamp timestamp) {
        activeSnapshots.computeIfPresent(timestamp.value(), (_, v) -> {
            int remaining = v.decrementAndGet();
            return remaining > 0 ? v : null;
        });
    }

    Timestamp gcWatermark() {
        Map.Entry<Long, AtomicInteger> oldest = activeSnapshots.firstEntry();
        return oldest != null ? new Timestamp(oldest.getKey()) : Timestamp.MAX;
    }

    int activeCount() {
        return activeSnapshots.values().stream()
                .mapToInt(AtomicInteger::get)
                .sum();
    }
}
