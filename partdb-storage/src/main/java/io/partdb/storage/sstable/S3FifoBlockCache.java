package io.partdb.storage.sstable;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class S3FifoBlockCache implements BlockCache {

    private record CacheKey(Path sstable, long offset) {}

    private static final class CacheEntry {
        final Block block;
        volatile int frequency;

        CacheEntry(Block block) {
            this.block = block;
            this.frequency = 0;
        }

        void access() {
            if (frequency < 3) {
                frequency++;
            }
        }
    }

    private final int smallCapacity;
    private final int mainCapacity;
    private final int ghostCapacity;

    private final Deque<CacheKey> smallQueue = new ArrayDeque<>();
    private final Map<CacheKey, CacheEntry> smallMap = new HashMap<>();

    private final Deque<CacheKey> mainQueue = new ArrayDeque<>();
    private final Map<CacheKey, CacheEntry> mainMap = new HashMap<>();

    private final Deque<CacheKey> ghostQueue = new ArrayDeque<>();
    private final Set<CacheKey> ghostSet = new HashSet<>();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public S3FifoBlockCache(int capacity) {
        this(capacity, 0.1);
    }

    public S3FifoBlockCache(int capacity, double smallRatio) {
        if (capacity < 10) {
            throw new IllegalArgumentException("capacity must be at least 10");
        }
        if (smallRatio <= 0 || smallRatio >= 1) {
            throw new IllegalArgumentException("smallRatio must be between 0 and 1");
        }
        this.smallCapacity = Math.max(1, (int) (capacity * smallRatio));
        this.mainCapacity = capacity - smallCapacity;
        this.ghostCapacity = capacity;
    }

    @Override
    public Optional<Block> get(Path sstable, BlockHandle handle) {
        CacheKey key = new CacheKey(sstable, handle.offset());

        lock.readLock().lock();
        try {
            CacheEntry entry = smallMap.get(key);
            if (entry != null) {
                entry.access();
                hits.incrementAndGet();
                return Optional.of(entry.block);
            }

            entry = mainMap.get(key);
            if (entry != null) {
                entry.access();
                hits.incrementAndGet();
                return Optional.of(entry.block);
            }

            misses.incrementAndGet();
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(Path sstable, BlockHandle handle, Block block) {
        CacheKey key = new CacheKey(sstable, handle.offset());

        lock.writeLock().lock();
        try {
            if (smallMap.containsKey(key) || mainMap.containsKey(key)) {
                return;
            }

            if (ghostSet.remove(key)) {
                ghostQueue.remove(key);
                insertToMain(key, new CacheEntry(block));
            } else {
                insertToSmall(key, new CacheEntry(block));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void invalidate(Path sstable) {
        lock.writeLock().lock();
        try {
            smallQueue.removeIf(k -> k.sstable().equals(sstable));
            smallMap.keySet().removeIf(k -> k.sstable().equals(sstable));

            mainQueue.removeIf(k -> k.sstable().equals(sstable));
            mainMap.keySet().removeIf(k -> k.sstable().equals(sstable));

            ghostQueue.removeIf(k -> k.sstable().equals(sstable));
            ghostSet.removeIf(k -> k.sstable().equals(sstable));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Stats stats() {
        lock.readLock().lock();
        try {
            return new Stats(
                hits.get(),
                misses.get(),
                evictions.get(),
                smallMap.size() + mainMap.size(),
                smallCapacity + mainCapacity
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    private void insertToSmall(CacheKey key, CacheEntry entry) {
        while (smallMap.size() >= smallCapacity) {
            evictFromSmall();
        }

        smallQueue.addLast(key);
        smallMap.put(key, entry);
    }

    private void insertToMain(CacheKey key, CacheEntry entry) {
        while (mainMap.size() >= mainCapacity) {
            evictFromMain();
        }

        mainQueue.addLast(key);
        mainMap.put(key, entry);
    }

    private void evictFromSmall() {
        while (!smallQueue.isEmpty()) {
            CacheKey key = smallQueue.pollFirst();
            CacheEntry entry = smallMap.remove(key);

            if (entry == null) {
                continue;
            }

            if (entry.frequency > 0) {
                entry.frequency = 0;
                insertToMain(key, entry);
            } else {
                addToGhost(key);
                evictions.incrementAndGet();
            }
            return;
        }
    }

    private void evictFromMain() {
        while (!mainQueue.isEmpty()) {
            CacheKey key = mainQueue.pollFirst();
            CacheEntry entry = mainMap.get(key);

            if (entry == null) {
                continue;
            }

            if (entry.frequency > 0) {
                entry.frequency--;
                mainQueue.addLast(key);
            } else {
                mainMap.remove(key);
                evictions.incrementAndGet();
                return;
            }
        }
    }

    private void addToGhost(CacheKey key) {
        while (ghostSet.size() >= ghostCapacity) {
            CacheKey old = ghostQueue.pollFirst();
            if (old != null) {
                ghostSet.remove(old);
            }
        }
        ghostQueue.addLast(key);
        ghostSet.add(key);
    }
}
