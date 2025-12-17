package io.partdb.storage.compaction;

import io.partdb.storage.sstable.SSTableDescriptor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public final class CompactionReservations {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Integer, Set<KeyRange>> rangesByLevel = new HashMap<>();
    private final Set<Long> reservedSSTableIds = new HashSet<>();

    public ReserveResult tryReserve(CompactionTask task) {
        lock.lock();
        try {
            Set<Long> taskIds = task.inputs().stream()
                .map(SSTableDescriptor::id)
                .collect(Collectors.toSet());

            if (!disjoint(reservedSSTableIds, taskIds)) {
                return new ReserveResult.Conflict();
            }

            KeyRange range = KeyRange.from(task.inputs());
            int sourceLevel = sourceLevel(task);
            int targetLevel = task.targetLevel();

            if (hasOverlap(sourceLevel, range) || hasOverlap(targetLevel, range)) {
                return new ReserveResult.Conflict();
            }

            reservedSSTableIds.addAll(taskIds);
            addRange(sourceLevel, range);
            if (sourceLevel != targetLevel) {
                addRange(targetLevel, range);
            }

            return new ReserveResult.Success(
                new ReservationToken(taskIds, sourceLevel, targetLevel, range)
            );
        } finally {
            lock.unlock();
        }
    }

    public void release(ReservationToken token) {
        lock.lock();
        try {
            reservedSSTableIds.removeAll(token.sstableIds());
            removeRange(token.sourceLevel(), token.keyRange());
            if (token.sourceLevel() != token.targetLevel()) {
                removeRange(token.targetLevel(), token.keyRange());
            }
        } finally {
            lock.unlock();
        }
    }

    public Set<Long> reservedSSTableIds() {
        lock.lock();
        try {
            return Set.copyOf(reservedSSTableIds);
        } finally {
            lock.unlock();
        }
    }

    public boolean hasActiveCompactions() {
        lock.lock();
        try {
            return !reservedSSTableIds.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    private boolean hasOverlap(int level, KeyRange range) {
        Set<KeyRange> levelRanges = rangesByLevel.get(level);
        if (levelRanges == null) {
            return false;
        }
        return levelRanges.stream().anyMatch(r -> r.overlaps(range));
    }

    private void addRange(int level, KeyRange range) {
        rangesByLevel.computeIfAbsent(level, _ -> new HashSet<>()).add(range);
    }

    private void removeRange(int level, KeyRange range) {
        Set<KeyRange> ranges = rangesByLevel.get(level);
        if (ranges != null) {
            ranges.remove(range);
            if (ranges.isEmpty()) {
                rangesByLevel.remove(level);
            }
        }
    }

    private static int sourceLevel(CompactionTask task) {
        return task.inputs().stream()
            .mapToInt(SSTableDescriptor::level)
            .min()
            .orElse(0);
    }

    private static boolean disjoint(Set<Long> a, Set<Long> b) {
        for (Long id : b) {
            if (a.contains(id)) {
                return false;
            }
        }
        return true;
    }
}
