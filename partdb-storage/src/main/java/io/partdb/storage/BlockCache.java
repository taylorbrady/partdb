package io.partdb.storage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class BlockCache {

    private static final double SMALL_QUEUE_RATIO = 0.1;
    private static final int GHOST_CAPACITY = 10_000;

    private record Key(long tableId, long offset) {}

    private static final class Slot {
        final Block block;
        final long sizeBytes;
        volatile int frequency;

        Slot(Block block) {
            this.block = block;
            this.sizeBytes = block.sizeBytes();
        }

        void touch() {
            if (frequency < 3) {
                frequency++;
            }
        }
    }

    private final long maxBytes;
    private final long maxSmallBytes;
    private final long maxMainBytes;
    private final Deque<Key> smallQueue = new ArrayDeque<>();
    private final Map<Key, Slot> small = new HashMap<>();
    private final Deque<Key> mainQueue = new ArrayDeque<>();
    private final Map<Key, Slot> main = new HashMap<>();
    private final LinkedHashSet<Key> ghost = new LinkedHashSet<>();
    private final Map<Long, Set<Key>> byTable = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long smallBytes;
    private long mainBytes;

    BlockCache(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative");
        }
        this.maxBytes = maxBytes;
        this.maxSmallBytes = (long) (maxBytes * SMALL_QUEUE_RATIO);
        this.maxMainBytes = maxBytes - maxSmallBytes;
    }

    Block get(long tableId, long offset) {
        if (maxBytes == 0) {
            return null;
        }
        Key key = new Key(tableId, offset);
        lock.readLock().lock();
        try {
            Slot slot = small.get(key);
            if (slot == null) {
                slot = main.get(key);
            }
            if (slot == null) {
                return null;
            }
            slot.touch();
            return slot.block;
        } finally {
            lock.readLock().unlock();
        }
    }

    void put(long tableId, long offset, Block block) {
        if (block.sizeBytes() > maxBytes) {
            return;
        }
        Key key = new Key(tableId, offset);
        lock.writeLock().lock();
        try {
            if (small.containsKey(key) || main.containsKey(key)) {
                return;
            }
            if (ghost.remove(key)) {
                insertMain(key, new Slot(block));
            } else {
                insertSmall(key, new Slot(block));
            }
            byTable.computeIfAbsent(tableId, _ -> new HashSet<>()).add(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void invalidate(long tableId) {
        if (maxBytes == 0) {
            return;
        }
        lock.writeLock().lock();
        try {
            Set<Key> keys = byTable.remove(tableId);
            if (keys == null) {
                return;
            }
            for (Key key : keys) {
                Slot slot = small.remove(key);
                if (slot != null) {
                    smallBytes -= slot.sizeBytes;
                }
                slot = main.remove(key);
                if (slot != null) {
                    mainBytes -= slot.sizeBytes;
                }
                ghost.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    long sizeBytes() {
        lock.readLock().lock();
        try {
            return smallBytes + mainBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void insertSmall(Key key, Slot slot) {
        while (smallBytes + slot.sizeBytes > maxSmallBytes && !small.isEmpty()) {
            evictSmall();
        }
        smallQueue.addLast(key);
        small.put(key, slot);
        smallBytes += slot.sizeBytes;
    }

    private void insertMain(Key key, Slot slot) {
        while (mainBytes + slot.sizeBytes > maxMainBytes && !main.isEmpty()) {
            evictMain();
        }
        mainQueue.addLast(key);
        main.put(key, slot);
        mainBytes += slot.sizeBytes;
    }

    private void evictSmall() {
        while (!smallQueue.isEmpty()) {
            Key key = smallQueue.pollFirst();
            Slot slot = small.remove(key);
            if (slot == null) {
                continue;
            }
            smallBytes -= slot.sizeBytes;
            if (slot.frequency > 0) {
                slot.frequency = 0;
                insertMain(key, slot);
            } else {
                unindex(key);
                remember(key);
            }
            return;
        }
    }

    private void evictMain() {
        while (!mainQueue.isEmpty()) {
            Key key = mainQueue.pollFirst();
            Slot slot = main.get(key);
            if (slot == null) {
                continue;
            }
            if (slot.frequency > 0) {
                slot.frequency--;
                mainQueue.addLast(key);
            } else {
                main.remove(key);
                mainBytes -= slot.sizeBytes;
                unindex(key);
                return;
            }
        }
    }

    private void unindex(Key key) {
        Set<Key> keys = byTable.get(key.tableId());
        if (keys != null && keys.remove(key) && keys.isEmpty()) {
            byTable.remove(key.tableId());
        }
    }

    private void remember(Key key) {
        if (ghost.size() >= GHOST_CAPACITY) {
            ghost.removeFirst();
        }
        ghost.add(key);
    }
}
