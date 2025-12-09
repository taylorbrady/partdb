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

    private static final double SMALL_QUEUE_RATIO = 0.1;
    private static final int GHOST_CAPACITY = 10_000;

    private record CacheKey(Path sstable, long offset) {}

    private static final class CacheEntry {
        final Block block;
        final long sizeInBytes;
        volatile int frequency;

        CacheEntry(Block block) {
            this.block = block;
            this.sizeInBytes = block.sizeInBytes();
            this.frequency = 0;
        }

        void access() {
            if (frequency < 3) {
                frequency++;
            }
        }
    }

    private final long maxSmallBytes;
    private final long maxMainBytes;
    private final long maxTotalBytes;

    private final Deque<CacheKey> smallQueue = new ArrayDeque<>();
    private final Map<CacheKey, CacheEntry> smallMap = new HashMap<>();
    private long smallBytes = 0;

    private final Deque<CacheKey> mainQueue = new ArrayDeque<>();
    private final Map<CacheKey, CacheEntry> mainMap = new HashMap<>();
    private long mainBytes = 0;

    private final Deque<CacheKey> ghostQueue = new ArrayDeque<>();
    private final Set<CacheKey> ghostSet = new HashSet<>();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public S3FifoBlockCache(long maxSizeInBytes) {
        if (maxSizeInBytes < 1024 * 1024) {
            throw new IllegalArgumentException("maxSizeInBytes must be at least 1MB");
        }
        this.maxTotalBytes = maxSizeInBytes;
        this.maxSmallBytes = (long) (maxSizeInBytes * SMALL_QUEUE_RATIO);
        this.maxMainBytes = maxSizeInBytes - maxSmallBytes;
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
        long blockSize = block.sizeInBytes();

        if (blockSize > maxTotalBytes) {
            return;
        }

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
            var smallIt = smallMap.entrySet().iterator();
            while (smallIt.hasNext()) {
                var entry = smallIt.next();
                if (entry.getKey().sstable().equals(sstable)) {
                    smallBytes -= entry.getValue().sizeInBytes;
                    smallIt.remove();
                }
            }
            smallQueue.removeIf(k -> k.sstable().equals(sstable));

            var mainIt = mainMap.entrySet().iterator();
            while (mainIt.hasNext()) {
                var entry = mainIt.next();
                if (entry.getKey().sstable().equals(sstable)) {
                    mainBytes -= entry.getValue().sizeInBytes;
                    mainIt.remove();
                }
            }
            mainQueue.removeIf(k -> k.sstable().equals(sstable));

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
                smallBytes + mainBytes,
                maxTotalBytes
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    private void insertToSmall(CacheKey key, CacheEntry entry) {
        while (smallBytes + entry.sizeInBytes > maxSmallBytes && !smallMap.isEmpty()) {
            evictFromSmall();
        }

        smallQueue.addLast(key);
        smallMap.put(key, entry);
        smallBytes += entry.sizeInBytes;
    }

    private void insertToMain(CacheKey key, CacheEntry entry) {
        while (mainBytes + entry.sizeInBytes > maxMainBytes && !mainMap.isEmpty()) {
            evictFromMain();
        }

        mainQueue.addLast(key);
        mainMap.put(key, entry);
        mainBytes += entry.sizeInBytes;
    }

    private void evictFromSmall() {
        while (!smallQueue.isEmpty()) {
            CacheKey key = smallQueue.pollFirst();
            CacheEntry entry = smallMap.remove(key);

            if (entry == null) {
                continue;
            }

            smallBytes -= entry.sizeInBytes;

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
                mainBytes -= entry.sizeInBytes;
                evictions.incrementAndGet();
                return;
            }
        }
    }

    private void addToGhost(CacheKey key) {
        while (ghostSet.size() >= GHOST_CAPACITY) {
            CacheKey old = ghostQueue.pollFirst();
            if (old != null) {
                ghostSet.remove(old);
            }
        }
        ghostQueue.addLast(key);
        ghostSet.add(key);
    }
}
